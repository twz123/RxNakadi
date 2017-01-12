package org.zalando.rxnakadi.internal;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.when;

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;

import org.mockito.Mock;

import org.mockito.runners.MockitoJUnitRunner;

import org.zalando.rxnakadi.SubscriptionEventBatch;
import org.zalando.rxnakadi.domain.Cursor;
import org.zalando.rxnakadi.domain.ImmutableCursor;
import org.zalando.rxnakadi.domain.ImmutableEventBatch;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import com.google.common.collect.Maps;

import rx.Completable;
import rx.Observable;
import rx.Observer;

import rx.observers.TestSubscriber;

import rx.schedulers.TestScheduler;

import rx.subjects.AsyncSubject;

@RunWith(MockitoJUnitRunner.class)
public class ManagedBatchOperatorTest {

    @Mock
    private NakadiHttpClient http;

    private final TestScheduler internalScheduler = new TestScheduler();
    private final TestScheduler externalScheduler = new TestScheduler();

    private Map.Entry<Optional<Cursor>, AsyncSubject<?>> currentCommit;

    @Before
    public void initializeTest() {
        when(http.commitCursor(any(), any(), any())).then(invocation -> {
            if (currentCommit != null) {
                return Completable.error(new AssertionError("Commit still in progress: " + currentCommit));
            }

            final AsyncSubject<?> commitSubject = AsyncSubject.create();
            currentCommit = Maps.immutableEntry(invocation.getArgument(0), commitSubject);
            return commitSubject.toCompletable();
        });
    }

    @After
    public void ensureNoPendingCommitRequests() {
        internalScheduler.triggerActions();
        assertThat("Another commit request happened unexpectedly.", currentCommit, is(nullValue()));
    }

    @Test
    public void coordinatesCommits() {
        final List<SubscriptionEventBatch<Object>> batches = listOfBatches(2);
        final TestSubscriber<?> firstSubscriber = commit(batches.get(0));
        final TestSubscriber<?> secondSubscriber = commit(batches.get(1));

        // the commit for the first batch should be in progress by now
        expectCommitRequestFor(batches.get(0).getCursor());
        processCommit(Observer::onCompleted);

        // the second commit should be in progress by now
        expectCommitRequestFor(batches.get(1).getCursor());

        // the first commit should complete on the external scheduler by now
        firstSubscriber.assertNoErrors();
        firstSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        firstSubscriber.assertNoErrors();
        firstSubscriber.assertCompleted();

        // process the second commit
        processCommit(Observer::onCompleted);

        // the second commit should complete on the external scheduler by now
        secondSubscriber.assertNoErrors();
        secondSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        secondSubscriber.assertNoErrors();
        secondSubscriber.assertCompleted();
    }

    @Test
    public void moreRecentCursorOutdatesEarlierOne() {
        final List<SubscriptionEventBatch<Object>> batches = listOfBatches(2);

        // commit the second, more recent batch first
        final TestSubscriber<?> secondSubscriber = commit(batches.get(1));

        // the commit for the second batch should be in progress by now
        expectCommitRequestFor(batches.get(1).getCursor());
        processCommit(Observer::onCompleted);

        // the second subscriber should complete on the external scheduler by now
        secondSubscriber.assertNoErrors();
        secondSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        secondSubscriber.assertNoErrors();
        secondSubscriber.assertCompleted();

        // the first batch is now outdated and doesn't need to be committed,
        // it should instantly complete on the external scheduler
        final TestSubscriber<?> firstSubscriber = commit(batches.get(0));
        firstSubscriber.assertNoErrors();
        firstSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        firstSubscriber.assertNoErrors();
        firstSubscriber.assertCompleted();
    }

    @Test
    public void doubleCommitOnSameBatchWorksSmoothly() {
        final SubscriptionEventBatch<Object> batch = listOfBatches(1).get(0);

        final TestSubscriber<?> firstSubscriber = commit(batch);
        final TestSubscriber<?> secondSubscriber = commit(batch);

        // the commit should be in progress by now
        expectCommitRequestFor(batch.getCursor());
        processCommit(Observer::onCompleted);

        // the batches are now committed and should complete on the external scheduler
        firstSubscriber.assertNoErrors();
        secondSubscriber.assertNoErrors();
        firstSubscriber.assertNoTerminalEvent();
        secondSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        firstSubscriber.assertNoErrors();
        secondSubscriber.assertNoErrors();
        firstSubscriber.assertCompleted();
        secondSubscriber.assertCompleted();
    }

    @Test
    public void propagatesFailure() {
        final RuntimeException commitFailed = new RuntimeException("Emulate error from HTTP layer!");

        final SubscriptionEventBatch<Object> batch = listOfBatches(1).get(0);

        final TestSubscriber<?> failingSubscriber = commit(batch);

        // the commit should be in progress by now
        expectCommitRequestFor(batch.getCursor());
        processCommit(observer -> observer.onError(commitFailed));

        // the error should now be propagated on the external scheduler
        failingSubscriber.assertNoErrors();
        failingSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        failingSubscriber.assertError(commitFailed);
        failingSubscriber.assertNotCompleted();

        // second try
        final TestSubscriber<?> succeessSubscriber = commit(batch);

        // the commit should be in progress by now
        expectCommitRequestFor(batch.getCursor());
        processCommit(Observer::onCompleted);

        // the succeeding subscriber should complete on the external scheduler by now
        succeessSubscriber.assertNoErrors();
        succeessSubscriber.assertNoTerminalEvent();
        externalScheduler.triggerActions();
        succeessSubscriber.assertNoErrors();
        succeessSubscriber.assertCompleted();
    }

    private static TestSubscriber<?> commit(final SubscriptionEventBatch<?> batch) {
        final TestSubscriber<?> subscriber = new TestSubscriber<>();
        batch.commit().subscribe(subscriber);
        return subscriber;
    }

    private void expectCommitRequestFor(final Cursor cursor) {
        internalScheduler.triggerActions();
        assertThat(currentCommit, is(hasProperty("key", hasValue(cursor))));
    }

    private void processCommit(final Consumer<? super Observer<?>> outcome) {
        outcome.accept(currentCommit.getValue());
        currentCommit = null;
        internalScheduler.triggerActions();
    }

    private <E> List<SubscriptionEventBatch<E>> listOfBatches(final int size) {
        return this.<E>streamOfBatches().take(size).toList().toBlocking().single();
    }

    private <E> Observable<SubscriptionEventBatch<E>> streamOfBatches() {
        final long nanoTime = System.nanoTime();
        final ManagedBatchOperator<E> underTest = new ManagedBatchOperator<>(http, "subscriptionId " + nanoTime,
                internalScheduler, externalScheduler);
        return Observable.interval(0, TimeUnit.NANOSECONDS)                                                            //
                         .doOnSubscribe(() ->
                                 underTest.setStreamId("streamId " + nanoTime + " " + System.nanoTime()))              //
                         .map(sequence -> ManagedBatchOperatorTest.<E>makeBatch("0", sequence.toString()))             //
                         .lift(underTest);
    }

    private static <E> ImmutableEventBatch<E> makeBatch(final String partition, final String offset) {
        return ImmutableEventBatch.<E>builder().cursor(makeCursor(partition, offset)).build();
    }

    private static ImmutableCursor makeCursor(final String partition, final String offset) {
        return ImmutableCursor.builder().partition(partition).offset(offset).build();
    }
}
