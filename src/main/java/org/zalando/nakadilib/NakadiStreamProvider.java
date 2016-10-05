package org.zalando.nakadilib;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.nakadilib.domain.Cursor;
import org.zalando.nakadilib.domain.EventBatch;
import org.zalando.nakadilib.domain.NakadiEvent;
import org.zalando.undertaking.oauth2.AccessToken;

import com.google.common.base.MoreObjects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;
import com.google.common.primitives.Ints;
import com.google.common.reflect.TypeToken;

import rx.Observable;
import rx.Single;
import rx.Subscriber;

/**
 * Provides instances of {@link Observable}s to retrieve Nakadi events of a particular type.
 */
public class NakadiStreamProvider {

    private static final class StreamContext<E> {
        final URI nakadiUrl;
        final String owningApplication;
        final EventType eventType;
        private final String consumerGroup;
        final TypeToken<E> eventClass;
        final long commitDelayMillis;

        private String toStringCache;

        StreamContext(final URI nakadiUrl, final String owningApplication, final EventType eventType,
                final String consumerGroup, final TypeToken<E> eventClass, final long commitDelayMillis) {
            this.nakadiUrl = requireNonNull(nakadiUrl);
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

    private final AsyncHttpClient httpClient;
    private final Single<AccessToken> accessToken;
    private final EventBatchParser parser;
    private final EventStreamSubscriptionProvider subscriptionProvider;
    private final CursorCommitter cursorCommitter;

    @Inject
    public NakadiStreamProvider(@Internal final AsyncHttpClient httpClient, final Single<AccessToken> accessToken,
            final EventStreamSubscriptionProvider subscriptionProvider, final EventBatchParser parser,
            final CursorCommitter cursorCommitter) {
        this.httpClient = requireNonNull(httpClient);
        this.accessToken = requireNonNull(accessToken);
        this.parser = requireNonNull(parser);
        this.subscriptionProvider = requireNonNull(subscriptionProvider);
        this.cursorCommitter = requireNonNull(cursorCommitter);
    }

    public <E extends NakadiEvent> Observable<List<E>> streamFor(final URI nakadiUrl, final String owningApplication,
                                                                 final EventType eventType, final String consumerGroup, final TypeToken<E> eventClass,
                                                                 final long commitDelayMillis) {
        return streamFor(new StreamContext<>(nakadiUrl, owningApplication, eventType, consumerGroup, eventClass,
                    commitDelayMillis));
    }

    private <E extends NakadiEvent> Observable<List<E>> streamFor(final StreamContext<E> ctx) {
        return
            subscriptionProvider.get(ctx.nakadiUrl, ctx.owningApplication, ctx.eventType, ctx.consumerGroup) //
                                .flatMapObservable(subscription ->
                                        createObservable(ctx, subscription))                                 //
                                .repeat()                                                                    //
                                .retryWhen(retryer -> scheduleRetries(retryer, ctx));
    }

    private <E extends NakadiEvent> Observable<List<E>> createObservable( //
            final StreamContext<E> ctx, final NakadiSubscription subscription) {

        final AtomicReference<Cursor> cursorRef = new AtomicReference<>();
        return accessToken.flatMapObservable(token ->
                    Observable.<EventBatch<E>>create(subscriber ->
                            onSubscribe(ctx, token, subscription, subscriber, cursorRef)) //
                    .doOnNext(batch -> cursorRef.set(batch.getCursor()))                //
                    .doOnSubscribe(() -> LOG.info("Starting event stream for [{}].", ctx)) //
                    .doOnCompleted(() -> LOG.info("Event stream completed for [{}].", ctx)) //
                    .map(EventBatch::getEvents));
    }

    private <E extends NakadiEvent> void onSubscribe(final StreamContext<E> ctx, final AccessToken token,
            final NakadiSubscription subscription, final Subscriber<? super EventBatch<E>> subscriber,
            final AtomicReference<Cursor> cursorRef) {

        final EventStreamHandler<E> handler = EventStreamHandler.<E>create(subscriber, parser.forType(ctx.eventClass));

        // Start the auto committer
        subscriber.add(cursorCommitter.autoCommit(ctx.nakadiUrl, handler.getSessionId(),
                () -> Optional.ofNullable(cursorRef.get()),
                subscription, ctx.commitDelayMillis));

        httpClient.prepareGet(getSubscriptionUrl(ctx, subscription))                  //
                  .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token.getValue()) //
                  .execute(handler);
    }

    private static String getSubscriptionUrl(final StreamContext<?> ctx, final NakadiSubscription subscription) {
        return String.format("%s/subscriptions/%s/events", ctx.nakadiUrl, subscription.getId());
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
