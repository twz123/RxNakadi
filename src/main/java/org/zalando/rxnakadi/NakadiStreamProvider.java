package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import com.google.common.base.MoreObjects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import rx.Observable;

/**
 * Provides instances of {@link Observable}s to retrieve Nakadi events of a particular type.
 */
public class NakadiStreamProvider {

    private static final class StreamContext<E> {
        final String owningApplication;
        final EventType eventType;
        private final String consumerGroup;
        final TypeToken<E> eventClass;
        final long commitDelayMillis;

        private String toStringCache;

        StreamContext(final String owningApplication, final EventType eventType, final String consumerGroup,
                final TypeToken<E> eventClass, final long commitDelayMillis) {
            this.owningApplication = requireNonNull(owningApplication);
            this.eventType = requireNonNull(eventType);
            this.consumerGroup = requireNonNull(consumerGroup);
            this.eventClass = requireNonNull(eventClass);
            checkArgument(commitDelayMillis >= 0, "commitDelayMillis may not be negative: %s", commitDelayMillis);
            this.commitDelayMillis = commitDelayMillis;
        }

        @Override
        public String toString() {
            String cache = toStringCache;
            if (cache == null) {
                cache =
                    MoreObjects.toStringHelper(this)                           //
                               .addValue(Integer.toHexString(hashCode()))      //
                               .addValue(owningApplication)                    //
                               .addValue(eventType)                            //
                               .addValue(consumerGroup)                        //
                               .addValue(commitDelayMillis + "ms auto commit") //
                               .toString();
                toStringCache = cache;
            }

            return cache;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(NakadiStreamProvider.class);

    private final EventBatchParser parser;
    private final NakadiHttpClient http;

    @Inject
    public NakadiStreamProvider(final NakadiHttpClient http, final EventBatchParser parser) {
        this.parser = requireNonNull(parser);
        this.http = requireNonNull(http);
    }

    public <E extends NakadiEvent> Observable<List<E>> streamFor(final String owningApplication,
            final EventType eventType, final String consumerGroup, final TypeToken<E> eventClass,
            final long commitDelayMillis) {
        return streamFor(new StreamContext<>(owningApplication, eventType, consumerGroup, eventClass,
                    commitDelayMillis));
    }

    private <E extends NakadiEvent> Observable<List<E>> streamFor(final StreamContext<E> ctx) {
        final Function<String, EventBatch<E>> eventParser = parser.forType(ctx.eventClass);

        return
            http.getSubscription(ctx.eventType, ctx.owningApplication, ctx.consumerGroup) //
                .flatMapObservable(subscription -> {
                    final String subscriptionId = subscription.getId();
                    final CursorAutoCommitter<E> cursorAutoCommitter =                    //
                        new CursorAutoCommitter<>(                                        //
                            http, subscriptionId,                                         //
                            ctx.commitDelayMillis, TimeUnit.MILLISECONDS);

                    return
                        http.getEventsForSubscription(subscriptionId, cursorAutoCommitter::setStreamId)     //
                        .map(eventParser::apply)                                                            //
                        .lift(cursorAutoCommitter)                                                          //
                        .doOnSubscribe(() -> LOG.info("Starting event stream for [{}].", ctx))              //
                        .doOnTerminate(() -> LOG.info("Event stream terminated for [{}].", ctx))            //
                        .map(EventBatch::getEvents);
                })                                                                                          //
                .repeat()                                                                                   //
                .retryWhen(retryer -> scheduleRetries(retryer, ctx));
    }

    private static Observable<?> scheduleRetries(final Observable<? extends Throwable> retryer,
            final StreamContext<?> ctx) {
        final Observable<Integer> retryDelays = Observable.from( //
                FluentIterable.from(Ints.asList(0, 1, 1, 5, 15, 30)).append(Iterables.cycle(60)));

        return retryer.zipWith(retryDelays, (error, delay) -> Maps.immutableEntry(error, delay)) //
                      .flatMap(entry -> {
                          final Throwable error = entry.getKey();
                          final int delay = entry.getValue();

                          if (delay > 0) {
                              LOG.error("Retrying event stream in [{}] seconds for [{}]: [{}]", delay, ctx,
                                  error.getMessage(), error);
                              return Observable.timer(delay, SECONDS);
                          }

                          LOG.error("Retrying event stream for [{}]: [{}]", ctx, error.getMessage(), error);
                          return Observable.just(0L);
                      });
    }

}
