package org.zalando.rxnakadi.internal;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.zalando.rxnakadi.SubscriptionEventBatch;
import org.zalando.rxnakadi.domain.Cursor;
import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.StreamInfo;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

import rx.Completable;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;

import rx.observables.ConnectableObservable;

import rx.schedulers.Schedulers;

import rx.subjects.AsyncSubject;

/**
 * Converts a stream of {@link EventBatch} instances into a stream of {@link SubscriptionEventBatch} instances.
 *
 * <p>This implementation coordinates commits:</p>
 *
 * <ul>
 *   <li>It ensures that only one commit at a time will be performed.</li>
 *   <li>Concurrent commit requests for cursors that were received before the one that's currently being committed will
 *     be grouped together and associated with the ongoing commit. They will complete upon completion of the ongoing
 *     request.</li>
 *   <li>Concurrent commit requests for cursors that were received after the one that's currently being committed will
 *     be grouped together and being committed right after the current commit terminates.</li>
 *   <li>Commit requests for cursors that have already been committed (e.g. through the successful commit of a more
 *     recently received cursor) will complete immediately.</li>
 * </ul>
 */
final class ManagedBatchOperator<E> implements Observable.Operator<SubscriptionEventBatch<E>, EventBatch<E>> {

    private final NakadiHttpClient http;
    private final String subscriptionId;

    /**
     * Used to observe emissions for internal use (draining).
     */
    private final ObserveOn internalObserveOn;

    /**
     * Used to observe emissions visible to the {@code Completable} that's returned from
     * {@link SubscriptionEventBatch#commit()}.
     */
    private final ObserveOn externalObserveOn;

    private final AtomicReference<String> streamIdRef = new AtomicReference<>();

    ManagedBatchOperator(final NakadiHttpClient http, final String subscriptionId) {
        this(http, subscriptionId, Schedulers.computation(), null);
    }

    @VisibleForTesting
    ManagedBatchOperator(final NakadiHttpClient http, final String subscriptionId, //
            final Scheduler internalScheduler, final Scheduler externalScheduler) {
        this.http = requireNonNull(http);
        checkArgument(!subscriptionId.isEmpty(), "subscriptionId may not be empty");
        this.subscriptionId = subscriptionId;
        this.internalObserveOn = ObserveOn.scheduler(internalScheduler);
        this.externalObserveOn = ObserveOn.scheduler(externalScheduler);
    }

    void setStreamId(final String streamId) {
        this.streamIdRef.set(streamId);
    }

    @FunctionalInterface
    private interface ObserveOn extends Observable.Transformer<Object, Object> {
        ObserveOn CALLING_THREAD = observable -> observable;

        static ObserveOn scheduler(final Scheduler scheduler) {
            return scheduler == null ? CALLING_THREAD : observable -> observable.observeOn(scheduler);
        }

        default Observable<Object> complete() {
            return Observable.empty().compose(this);
        }
    }

    @Override
    public Subscriber<? super EventBatch<E>> call(final Subscriber<? super SubscriptionEventBatch<E>> child) {
        final Committer committer = new Committer( //
                http, subscriptionId, streamIdRef, internalObserveOn, externalObserveOn);
        return new ManagedBatchSubscriber(child, committer);
    }

    private final class ManagedBatchSubscriber extends Subscriber<EventBatch<E>> {

        private final Subscriber<? super SubscriptionEventBatch<E>> child;

        private long sequencer;
        private final Committer committer;

        ManagedBatchSubscriber(final Subscriber<? super SubscriptionEventBatch<E>> child, final Committer committer) {
            super(child);
            this.child = child;
            this.committer = committer;
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
            child.onNext(new ManagedBatch<>(committer, item, ++sequencer));
        }
    }

    private static final class ManagedBatch<E> implements SubscriptionEventBatch<E> {
        private final Committer committer;
        private final EventBatch<E> batch;
        final long sequence;

        ManagedBatch(final Committer committer, final EventBatch<E> batch, final long sequence) {
            this.committer = committer;
            this.batch = batch;
            this.sequence = sequence;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(sequence).addValue(batch).toString();
        }

        @Override
        public Cursor getCursor() {
            return batch.getCursor();
        }

        @Override
        public List<E> getEvents() {
            return batch.getEvents();
        }

        @Override
        public StreamInfo getInfo() {
            return batch.getInfo();
        }

        @Override
        public Completable commit() {
            return committer.commit(this);
        }
    }

    @SuppressWarnings("serial")
    private static final class CommitBulk extends AtomicReference<ManagedBatch<?>> {

        private static final ManagedBatch<?> EMPTY = new ManagedBatch<>(null, null, Long.MIN_VALUE);
        private static final ManagedBatch<?> DRAINED = new ManagedBatch<>(null, null, Long.MAX_VALUE);

        final Observer<? super Object> observer;
        final Completable completable;

        CommitBulk() {
            super(EMPTY);

            final AsyncSubject<? super Object> subject = AsyncSubject.create();

            observer = subject;
            completable = subject.toCompletable();
        }

        /**
         * @return  {@code false} if this bulk has been drained, {@code true} otherwise
         */
        boolean accept(final ManagedBatch<?> batch) {
            while (true) { // CAS to accept the given batch

                final ManagedBatch<?> current = get();

                if (current == DRAINED) {
                    return false;
                }

                if (current != EMPTY && batch.sequence <= current.sequence) {
                    return true;
                }

                if (compareAndSet(current, batch)) {
                    return true;
                }
            }
        }

        Optional<ManagedBatch<?>> drain() {
            while (true) { // CAS to drain this bulk

                final ManagedBatch<?> current = get();

                // If EMPTY, there's nothing to do, so no need to set this to DRAINED
                // If DRAINED, it's drained (terminal state)
                if (current == EMPTY || current == DRAINED) {
                    return Optional.empty();
                }

                if (compareAndSet(current, DRAINED)) {
                    return Optional.of(current);
                }
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class Committer extends AtomicReference<CommitBulk> {
        private final NakadiHttpClient http;
        private final String subscriptionId;
        private final AtomicReference<String> streamIdRef;

        /**
         * @see  ManagedBatchOperator#internalObserveOn
         */
        private final ObserveOn internalObserveOn;

        /**
         * @see  ManagedBatchOperator#externalObserveOn
         */
        private final ObserveOn externalObserveOn;

        // Holds a reference to the bulk that is currently being committed, if any.
        private final AtomicReference<CommitBulk> committingBulk = new AtomicReference<>();

        private volatile long smallestUncommittedSequence = Long.MIN_VALUE;

        Committer(final NakadiHttpClient http, final String subscriptionId, final AtomicReference<String> streamIdRef,
                final ObserveOn internalObserveOn, final ObserveOn externalObserveOn) {
            super(new CommitBulk());
            this.http = http;
            this.subscriptionId = subscriptionId;
            this.streamIdRef = streamIdRef;
            this.internalObserveOn = internalObserveOn;
            this.externalObserveOn = externalObserveOn;
        }

        Completable commit(final ManagedBatch<?> batch) {

            // Fast path for batches that have already been committed via another, more recent cursor.
            if (batch.sequence < smallestUncommittedSequence) {
                return externalObserveOn.complete().toCompletable();
            }

            CommitBulk newBulk = null;
            while (true) { // CAS for drained bulks

                // Get the current bulk that accepts new batches.
                final CommitBulk bulk = get();

                if (bulk.accept(batch)) {

                    // The bulk accepted the cursor. Start a drain and return.
                    drain();
                    return bulk.completable;

                }

                // Bulk is drained. Try to swap in a fresh empty bulk and start over CASing.
                compareAndSet(bulk, newBulk == null ? (newBulk = new CommitBulk()) : newBulk);
            }
        }

        void drain() {
            CommitBulk bulk;
            while (true) { // CAS for bulks currently being committed

                // There's currently a commit going on, so don't start another commit concurrently.
                if (committingBulk.get() != null) {
                    return;
                }

                bulk = get();
                if (committingBulk.compareAndSet(null, bulk)) {
                    break;
                }
            }

            boolean reset = true;
            try {
                reset = !commitBulk(bulk);
            } finally {
                if (reset) {
                    committingBulk.compareAndSet(bulk, null);
                }
            }
        }

        private boolean commitBulk(final CommitBulk bulk) {
            final Optional<ManagedBatch<?>> drained = bulk.drain();
            if (!drained.isPresent()) { // No batch to commit in this bulk.
                return false;
            }

            final ManagedBatch<?> batch = drained.get();

            // Check if the cursor has already been committed concurrently. This may happen when requesting a batch
            // to be committed while another commit with a higher sequence is being executed, but not yet complete.
            if (batch.sequence < smallestUncommittedSequence) {
                externalObserveOn.complete().subscribe(bulk.observer);
                return false;
            }

            final Optional<Cursor> cursor = Optional.ofNullable(batch.getCursor());
            final long nextUncommittedSequence = batch.sequence + 1;

            // Setup a connectable observable for the commit.
            final ConnectableObservable<?> commit = //
                Observable.defer(() -> {
                              final String streamId = streamIdRef.get();
                              checkState(streamId != null, "No stream ID!");
                              return http.commitCursor(cursor, subscriptionId, streamId).toObservable();
                          }).publish();

            // Hook in our own callbacks that record the outcome and start over another drain.
            commit.compose(internalObserveOn) //
                  .doOnCompleted(() -> {

                      // No need to CAS here, since this code block will never be evaluated in parallel.
                      // This is because we're serializing commits to happen one after another via CASing on
                      // committingBulk.
                      if (nextUncommittedSequence > smallestUncommittedSequence) {
                          smallestUncommittedSequence = nextUncommittedSequence;
                      }
                  })                                     //
                  .onErrorResumeNext(Observable.empty()) // We don't care about errors here, users should handle those
                  .doOnTerminate(() -> {

                      // The commit is finished. Release the "lock" and start another drain run.
                      committingBulk.compareAndSet(bulk, null);
                      drain();
                  }) //
                  .subscribe();

            // Hook in the bulk's observer.
            commit.compose(externalObserveOn).subscribe(bulk.observer);

            // Connect the commit observable. The hooked in subscribers will be called concurrently.
            commit.connect();

            return true;
        }
    }
}
