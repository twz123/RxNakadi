package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
        final CursorCapturingSubscriber<E> parent = new CursorCapturingSubscriber<>(child);

        // Start the auto committer
        child.add(autoCommit(parent));

        return parent;
    }

    private Subscription autoCommit(final Supplier<Object> cursorSupplier) {

        return Completable.defer(() -> {
                              final String streamId = CursorAutoCommitter.this.streamId;
                              checkState(streamId != null, "No stream ID!");

                              final Optional<Object> cursor = Optional.ofNullable(cursorSupplier.get());

                              return
                                  http.commitCursor(cursor, subscriptionId, streamId)                                                        //
                                  .doOnSubscribe(subscription ->
                                          LOG.debug("Committing cursor [{}] on subscription [{}] for stream [{}]…",                          //
                                              cursor, subscriptionId, streamId))                                                             //
                                  .doOnCompleted(() ->
                                          LOG.info("Committed cursor [{}] on subscription [{}] for stream [{}]",                             //
                                              cursor, subscriptionId, streamId))                                                             //
                                  .onErrorComplete(error -> {
                                      LOG.error(
                                          "Failed to commit cursor [{}] on subscription [{}] for stream [{}]: [{}]",                         //
                                          cursor, subscriptionId, streamId, error.getMessage(), error);
                                      return true;
                                  });                                                                                                        //
                          })                                                                                                                 //
                          .onErrorComplete(error -> {
                              LOG.error("Failed to commit cursor automatically on subscription [{}]: [{}]",                                  //
                                  subscriptionId, error.getMessage(), error);
                              return true;
                          })                                                                                                                 //
                          .repeatWhen(redo -> redo.flatMap(next -> Observable.timer(delay, unit)))                                           //
                          .compose(autoCommitter -> Completable.timer(delay, unit).andThen(autoCommitter))
                          .doOnSubscribe(subscription ->
                                  LOG.info("Automatically committing cursors for subscription [{}] every [{} {}].",                          //
                                      subscriptionId, delay, unit))                                                                          //
                          .doOnUnsubscribe(() ->
                                  LOG.debug("Unsubscribed from auto committing cursors for subscription [{}].",
                                      subscriptionId))                                                                                       //
                          .doOnTerminate(() ->
                                  LOG.info("Ending auto committing cursors for subscription [{}].", subscriptionId))                         //
                          .subscribe();
    }

    private static final class CursorCapturingSubscriber<E> extends Subscriber<EventBatch<E>>
        implements Supplier<Object> {
        private final Subscriber<? super EventBatch<E>> child;

        private volatile Object cursor;

        CursorCapturingSubscriber(final Subscriber<? super EventBatch<E>> child) {
            super(child);
            this.child = child;
        }

        @Override
        public Object get() {
            return cursor;
        }

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
    }
}
