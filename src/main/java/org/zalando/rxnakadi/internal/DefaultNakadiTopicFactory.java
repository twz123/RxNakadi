package org.zalando.rxnakadi.internal;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.function.Function;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.AutoCommit;
import org.zalando.rxnakadi.NakadiTopic;
import org.zalando.rxnakadi.NakadiTopicFactory;
import org.zalando.rxnakadi.SubscriptionDescriptor;
import org.zalando.rxnakadi.TopicDescriptor;
import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import rx.Completable;
import rx.Observable;

public class DefaultNakadiTopicFactory implements NakadiTopicFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultNakadiTopicFactory.class);

    private final NakadiHttpClient http;
    private final EventBatchParser parser;

    @Inject
    DefaultNakadiTopicFactory(final NakadiHttpClient http, final EventBatchParser parser) {
        this.parser = requireNonNull(parser);
        this.http = requireNonNull(http);
    }

    @Override
    public <E extends NakadiEvent> NakadiTopic<E> create(final TopicDescriptor<E> descriptor) {
        requireNonNull(descriptor);

        return new NakadiTopic<E>() {
            @Override
            public Observable<List<E>> events(final SubscriptionDescriptor sd, final AutoCommit ac) {
                return createEventSource(descriptor, sd, ac);
            }

            @Override
            public Completable publish(final List<E> events) {
                return http.publishEvents(descriptor.getEventType(), events);
            }
        };
    }

    <E extends NakadiEvent> Observable<List<E>> createEventSource(final TopicDescriptor<E> td,
            final SubscriptionDescriptor sd, final AutoCommit autoCommit) {
        final Function<String, EventBatch<E>> eventParser = parser.forType(td.getEventTypeToken());

        return http.getSubscription(td.getEventType(), sd)         //
                   .flatMapObservable(subscription -> {
                       final String subscriptionId = subscription.getId();
                       final CursorAutoCommitter<E> cursorAutoCommitter = //
                           new CursorAutoCommitter<>(http, subscriptionId, autoCommit);

                       return
                           http.getEventsForSubscription(subscriptionId, cursorAutoCommitter::setStreamId)       //
                           .map(eventParser::apply)                                                              //
                           .lift(cursorAutoCommitter)                                                            //
                           .doOnSubscribe(() -> LOG.info("Starting event stream on [{}] for [{}].", sd, td))     //
                           .doOnTerminate(() -> LOG.info("Event stream terminated on [{}] for [{}].", sd, td))   //
                           .map(EventBatch::getEvents);
                   })                                                                                            //
                   .repeat()                                                                                     //
                   .retryWhen(retryer -> scheduleStreamRetries(retryer, td, sd));
    }

    private static Observable<?> scheduleStreamRetries(final Observable<? extends Throwable> retryer,
            final TopicDescriptor<?> td, final SubscriptionDescriptor sd) {
        final Observable<Integer> retryDelays = Observable.from( //
                FluentIterable.from(Ints.asList(0, 1, 1, 5, 15, 30)).append(Iterables.cycle(60)));

        return retryer.zipWith(retryDelays, (error, delay) -> Maps.immutableEntry(error, delay)) //
                      .flatMap(entry -> {
                          final Throwable error = entry.getKey();
                          final int delay = entry.getValue();

                          if (delay > 0) {
                              LOG.error("Retrying event stream in [{}] seconds on [{}] for [{}]: [{}]", delay, sd, td,
                                  error.getMessage(), error);
                              return Observable.timer(delay, SECONDS);
                          }

                          LOG.error("Retrying event stream on [{}] for [{}]: [{}]", sd, td, error.getMessage(), error);
                          return Observable.just(0L);
                      });
    }
}
