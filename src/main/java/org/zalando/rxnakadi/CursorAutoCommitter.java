package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import rx.Completable;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

final class CursorAutoCommitter<E> implements Observable.Operator<EventBatch<E>, EventBatch<E>> {

    private static final Logger LOG = LoggerFactory.getLogger(CursorAutoCommitter.class);

    private final NakadiHttpClient http;
    private final String subscriptionId;
    private final long delay;
    private final TimeUnit unit;

    private volatile String streamId;
    private volatile Object cursor;

    CursorAutoCommitter(final NakadiHttpClient http, final String subscriptionId, //
            final long delay, final TimeUnit unit) {
        this.http = requireNonNull(http);
        checkArgument(!subscriptionId.isEmpty(), "subscriptionId may not be empty");
        this.subscriptionId = subscriptionId;
        checkArgument(delay > 0, "delay must be positive: %s", delay);
        this.delay = delay;
        this.unit = requireNonNull(unit);
    }

    void setStreamId(final String streamId) {
        this.streamId = streamId;
    }

    @Override
    public Subscriber<? super EventBatch<E>> call(final Subscriber<? super EventBatch<E>> child) {
        final Subscriber<EventBatch<E>> subscriber = new Subscriber<EventBatch<E>>(child) {
            @Override
            public void onCompleted() {
                child.onCompleted();
            }

            @Override
            public void onError(final Throwable t) {
                child.onError(t);
            }

            @Override
            public void onNext(final EventBatch<E> item) {
                cursor = item.getCursor();
                child.onNext(item);
            }
        };

        // Start the auto committer
        subscriber.add(autoCommit());

        return subscriber;
    }

    private Subscription autoCommit() {
        return Observable.defer(() -> {
                             final String streamId = CursorAutoCommitter.this.streamId;
                             checkState(streamId != null, "No stream ID!");

                             final Optional<Object> cursor = Optional.ofNullable(CursorAutoCommitter.this.cursor);

                             return http.commitCursor(cursor, subscriptionId, streamId)                                                     //
                                 .doOnSubscribe(() ->
                                         LOG.debug("Committing cursor [{}] on subscription [{}] for stream [{}]…",                          //
                                             cursor, subscriptionId, streamId))                                                             //
                                 .doOnSuccess(result ->
                                         LOG.info("Committed cursor [{}] on subscription [{}] for stream [{}]: [{}]",                       //
                                             cursor, subscriptionId, streamId, result))                                                     //
                                 .toObservable()                                                                                            //
                                 .onErrorResumeNext(error -> {
                                     LOG.error(
                                         "Failed to commit cursor [{}] on subscription [{}] for stream [{}]: [{}]",                         //
                                         cursor, subscriptionId, streamId, error.getMessage(), error);
                                     return Observable.empty();
                                 });                                                                                                        //
                         })                                                                                                                 //
                         .onErrorResumeNext(error -> {
                             LOG.error("Failed to commit cursor [{}] on subscription [{}]: [{}]",                                           //
                                 cursor, subscriptionId, error.getMessage(), error);
                             return Observable.empty();
                         })                                                                                                                 //
                         .repeatWhen(redo -> redo.flatMap(next -> Observable.timer(delay, unit)))                                           //
                         .compose(autoCommitter -> Completable.timer(delay, unit).andThen(autoCommitter))
                         .doOnSubscribe(() ->
                                 LOG.info("Auto commit cursors for subscription [{}] every [{} {}].",                                       //
                                     subscriptionId, delay, unit))                                                                          //
                         .doOnUnsubscribe(() ->
                                 LOG.debug("Unsubscribed from auto committing cursors for subscription [{}].",
                                     subscriptionId))                                                                                       //
                         .doOnTerminate(() ->
                                 LOG.info("Ending auto committing cursors for subscription [{}].", subscriptionId))                         //
                         .subscribe();
    }
}
