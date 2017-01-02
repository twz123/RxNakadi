package org.zalando.rxnakadi.internal;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.AutoCommit;
import org.zalando.rxnakadi.EventType;
import org.zalando.rxnakadi.NakadiTopic;
import org.zalando.rxnakadi.NakadiTopicFactory;
import org.zalando.rxnakadi.StreamOffsets;
import org.zalando.rxnakadi.SubscriptionDescriptor;
import org.zalando.rxnakadi.TopicDescriptor;
import org.zalando.rxnakadi.domain.Cursor;
import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import rx.Completable;
import rx.Observable;

public class DefaultNakadiTopicFactory implements NakadiTopicFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultNakadiTopicFactory.class);

    private final NakadiHttpClient http;
    private final JsonCoder json;

    @Inject
    DefaultNakadiTopicFactory(final NakadiHttpClient http, final JsonCoder json) {
        this.http = requireNonNull(http);
        this.json = requireNonNull(json);
    }

    @Override
    public <E extends NakadiEvent> NakadiTopic<E> create(final TopicDescriptor<E> descriptor) {
        requireNonNull(descriptor);

        return new NakadiTopic<E>() {
            @Override
            public Observable<EventBatch<E>> events() {
                return createEventSource(descriptor, Observable.empty());
            }

            @Override
            public Observable<EventBatch<E>> events(final StreamOffsets offsets) {
                return createEventSource(descriptor, getCursors(descriptor.getEventType(), requireNonNull(offsets)));
            }

            @Override
            public Observable<List<E>> events(final SubscriptionDescriptor sd, final AutoCommit ac) {
                return createEventSource(descriptor, requireNonNull(sd), requireNonNull(ac));
            }

            @Override
            public Completable publish(final List<E> events) {
                return http.publishEvents(descriptor.getEventType(), events);
            }
        };
    }

    private <E> Observable<EventBatch<E>> createEventSource(final TopicDescriptor<E> td,
            final Observable<List<Cursor>> cursorSource) {

        final EventType eventType = td.getEventType();
        final String streamDescription = td + " (" + Long.toHexString(System.nanoTime()) + ')';

        return cursorSource.flatMap(cursors ->
                    http.getEventsForType(eventType, cursors)                         //
                    .compose(logLifecycle(streamDescription + " with " + cursors))    //
                    .switchIfEmpty(http.getEventsForType(eventType)                   //
                        .compose(logLifecycle(streamDescription)))                    //
                    .compose(parseEventChunks(td.getEventTypeToken(), streamDescription)) //
                    .compose(repeatAndRetry(streamDescription)));
    }

    private <E extends NakadiEvent> Observable<List<E>> createEventSource(final TopicDescriptor<E> td,
            final SubscriptionDescriptor sd, final AutoCommit autoCommit) {

        final String streamDescription = td + " for " + sd + " (" + Long.toHexString(System.nanoTime()) + ')';

        return http.getSubscription(td.getEventType(), sd)         //
                   .flatMapObservable(subscription -> {
                       final String subscriptionId = subscription.getId();
                       final CursorAutoCommitter<E> cursorAutoCommitter = //
                           new CursorAutoCommitter<>(http, subscriptionId, autoCommit);

                       return
                           http.getEventsForSubscription(subscriptionId, cursorAutoCommitter::setStreamId)         //
                           .compose(parseEventChunks(td.getEventTypeToken(), streamDescription))                   //
                           .lift(cursorAutoCommitter)                                                              //
                           .compose(logLifecycle(streamDescription));                                              //
                   })                                                                                              //
                   .map(EventBatch::getEvents)                                                                     //
                   .filter(events -> events != null && !events.isEmpty())                                          //
                   .compose(repeatAndRetry(streamDescription));
    }

    private Observable<List<Cursor>> getCursors(final EventType eventType, final StreamOffsets offsets) {
        return http.getPartitions(eventType)                                                               //
                   .flatMapObservable(Observable::from)                                                    //
                   .flatMap(partition ->
                           offsets.offsetFor(partition)                                                    //
                           .toObservable()                                                                 //
                           .filter(Optional::isPresent)                                                    //
                           .map(Optional::get)                                                             //
                           .map(offset -> new Cursor().setPartition(partition.getPartition()).setOffset(offset))) //
                   .toList();
    }

    private static <T> Observable.Transformer<T, T> logLifecycle(final String streamDescription) {
        return o -> {
            return o.doOnSubscribe(() -> LOG.info("Starting event stream on [{}].", streamDescription)) //
                    .doOnTerminate(() -> LOG.info("Event stream terminated on [{}].", streamDescription));
        };
    }

    private <E> Observable.Transformer<String, EventBatch<E>> parseEventChunks(final TypeToken<E> eventTypeToken,
            final String streamDescription) {
        @SuppressWarnings("serial")
        final TypeToken<EventBatch<E>> eventBatchToken =
            new TypeToken<EventBatch<E>>() { }.where(new TypeParameter<E>() { }, eventTypeToken);
        return o -> o.compose(traceEventChunks(streamDescription)).map(chunk -> json.fromJson(chunk, eventBatchToken));
    }

    private static <T> Observable.Transformer<T, T> traceEventChunks(final String streamDescription) {
        return o ->
                LOG.isTraceEnabled() ?                                                                          //
                o.doOnNext(chunk -> LOG.trace("Received JSON chunk on [{}]: [{}]", streamDescription, chunk)) : //
                o;
    }

    private static <T> Observable.Transformer<T, T> repeatAndRetry(final String streamDescription) {
        return o -> o.repeat().retryWhen(retryer -> scheduleStreamRetries(retryer, streamDescription));
    }

    private static Observable<?> scheduleStreamRetries(final Observable<? extends Throwable> retryer,
            final String streamDescription) {
        final Observable<Integer> retryDelays = Observable.from( //
                FluentIterable.from(Ints.asList(0, 1, 1, 5, 15, 30)).append(Iterables.cycle(60)));

        return retryer.zipWith(retryDelays, (error, delay) -> Maps.immutableEntry(error, delay)) //
                      .flatMap(entry -> {
                          final Throwable error = entry.getKey();
                          final int delay = entry.getValue();

                          if (delay > 0) {
                              LOG.error("Retrying event stream in [{}] seconds on [{}]: [{}]", delay, streamDescription,
                                  error.getMessage(), error);
                              return Observable.timer(delay, SECONDS);
                          }

                          LOG.error("Retrying event stream on [{}]: [{}]", streamDescription, error.getMessage(), error);
                          return Observable.just(0L);
                      });
    }
}
