package org.zalando.hystrix;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import static org.hobsoft.hamcrest.compose.ComposeMatchers.hasFeature;

import static org.junit.Assert.fail;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hamcrest.Matcher;

import org.junit.Rule;
import org.junit.Test;

import org.junit.internal.matchers.ThrowableMessageMatcher;

import org.junit.rules.ExpectedException;

import org.zalando.rxnakadi.hystrix.HystrixCommands;

import com.google.common.base.Throwables;

import com.netflix.hystrix.HystrixInvokableInfo;
import com.netflix.hystrix.HystrixObservable;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

import rx.Single;

import rx.observers.TestSubscriber;

public class HystrixCommandsTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final Single<String> timeout = //
        mockHystrixRuntimeException(HystrixRuntimeException.FailureType.TIMEOUT);

    private final Single<String> cmdException = //
        mockHystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION);

    private final Single<String> shortCircuit = //
        mockHystrixRuntimeException(HystrixRuntimeException.FailureType.SHORTCIRCUIT);

    private final Single<String> badReqException = mockException(HystrixBadRequestException.class);

    private interface CommandMock<T> extends HystrixObservable<T>, HystrixInvokableInfo<T> {
        // combining interface to ease mocking
    }

    @SuppressWarnings("serial")
    private static final class MockedException extends RuntimeException {

        final Object attribute;

        public MockedException(final String message, final Object attribute) {
            super(message);
            this.attribute = attribute;
        }

        @Override
        public String toString() {
            return "mocked exception: " + getMessage();
        }
    }

    @Test
    public void unableToInstantiate() throws ReflectiveOperationException {
        final Constructor<HystrixCommands> ctor = HystrixCommands.class.getDeclaredConstructor();
        ctor.setAccessible(true);

        try {
            ctor.newInstance();
        } catch (final InvocationTargetException e) {
            assertThat(e.getCause(),
                allOf(instanceOf(AssertionError.class),
                    ThrowableMessageMatcher.hasMessage(is("No instances for you!"))));
            return;
        }

        fail("An expected exception has not been thrown!");
    }

    @Test
    public void eagerlyFailsOnNonPositiveMaxAttempts() {
        final Callable<CommandMock<Void>> commandFactory = mock(Callable.class);

        expected.expect(IllegalArgumentException.class);
        expected.expectMessage(is("maxAttempts must be positive: 0"));

        HystrixCommands.withRetries(commandFactory, 0);
    }

    @Test
    public void retriesSomeHystrixRuntimeExceptions() throws Exception {
        final Callable<CommandMock<String>> factory = mockCommandFactory(timeout, cmdException, shortCircuit);

        final TestSubscriber<String> subscriber = retry(factory, 5);

        subscriber.awaitTerminalEvent();

        // exactly three calls wanted
        verify(factory, times(3)).call();

        expected.expect(MockedException.class);
        expected.expect(attribute(is(HystrixRuntimeException.FailureType.SHORTCIRCUIT)));

        assertAndThrowError(subscriber);
    }

    @Test
    public void noRetryForBadRequestException() throws Exception {
        final Callable<CommandMock<String>> factory = mockCommandFactory(badReqException, Single.just("ok"));

        final TestSubscriber<String> subscriber = retry(factory, 2);

        subscriber.awaitTerminalEvent();

        // exactly once wanted
        verify(factory).call();

        expected.expect(MockedException.class);
        expected.expect(attribute(is(sameInstance(HystrixBadRequestException.class))));

        assertAndThrowError(subscriber);
    }

    @Test
    public void stopsAfterMaxAttempts() throws Exception {
        final Callable<CommandMock<String>> factory = mockCommandFactory(timeout, timeout, cmdException);

        final TestSubscriber<?> subscriber = retry(factory, 3);

        subscriber.awaitTerminalEvent();

        // exactly three calls wanted
        verify(factory, times(3)).call();

        expected.expect(MockedException.class);
        expected.expect(attribute(is(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION)));

        assertAndThrowError(subscriber);
    }

    @Test
    public void propagatesExceptionsIfCallFails() {

        final RuntimeException broken = new RuntimeException("commandFactory is horribly broken");

        final TestSubscriber<?> subscriber = retry(() -> { throw broken; }, 1);

        subscriber.awaitTerminalEvent();

        expected.expect(is(sameInstance(broken)));

        assertAndThrowError(subscriber);
    }

    @Test
    public void unwrapsExceptionIfSingleAttempt() throws Exception {

        final Callable<CommandMock<String>> factory = mockCommandFactory(badReqException);

        final TestSubscriber<?> subscriber = retry(factory, 1);

        subscriber.awaitTerminalEvent();

        // exactly once wanted
        verify(factory).call();

        expected.expect(MockedException.class);
        expected.expect(attribute(is(sameInstance(HystrixBadRequestException.class))));

        assertAndThrowError(subscriber);
    }

    @Test
    public void propagatesExceptionAsIsIfNoCause() throws Exception {

        final HystrixBadRequestException noCause = mock(HystrixBadRequestException.class);

        final Callable<CommandMock<String>> factory = mockCommandFactory(Single.error(noCause));

        final TestSubscriber<?> subscriber = retry(factory, 1);

        subscriber.awaitTerminalEvent();

        // exactly once wanted
        verify(factory).call();

        expected.expect(is(sameInstance(noCause)));

        assertAndThrowError(subscriber);
    }

    @SafeVarargs
    private static <T> Callable<CommandMock<T>> mockCommandFactory(final Single<? extends T>... singles)
        throws Exception {
        return mockCommandFactory(Arrays.asList(singles));
    }

    private static <T> Callable<CommandMock<T>> mockCommandFactory(final Collection<Single<? extends T>> singles)
        throws Exception {
        final Callable<CommandMock<T>> mock = mock(Callable.class);

        final Deque<Single<? extends T>> queue = new ConcurrentLinkedDeque<>(singles);
        when(mock.call()).then(invocation -> mockCommand(queue.pop()));

        return mock;
    }

    private static <T> CommandMock<T> mockCommand(final Single<? extends T> single) {
        final AtomicBoolean called = new AtomicBoolean();
        final CommandMock<T> mock = mock(CommandMock.class, RETURNS_DEEP_STUBS);

        when(mock.toObservable()).then(invocation -> {
            if (called.compareAndSet(false, true)) {
                return single.toObservable();
            }

            throw new AssertionError("unexpected call");
        });

        when(mock.getCommandGroup().name()).thenReturn("mockedGroup");
        when(mock.getCommandKey().name()).thenReturn("mockedCommand");

        return mock;
    }

    private static <T> TestSubscriber<T> retry(final Callable<CommandMock<T>> commandFactory, final int maxAttempts) {
        final TestSubscriber<T> subscriber = new TestSubscriber<>();
        HystrixCommands.withRetries(commandFactory, maxAttempts).subscribe(subscriber);
        return subscriber;
    }

    private static <T> Single<T> mockHystrixRuntimeException(final HystrixRuntimeException.FailureType failureType) {
        final HystrixRuntimeException mock = mock(HystrixRuntimeException.class);
        when(mock.getFailureType()).thenReturn(failureType);
        when(mock.getCause()).thenReturn(new MockedException(failureType.toString(), failureType));
        return Single.error(mock);
    }

    private static <T, X extends Throwable> Single<T> mockException(final Class<X> clazz) {
        final X mock = mock(clazz);
        when(mock.getCause()).thenReturn(new MockedException(clazz.getSimpleName(), clazz));
        return Single.error(mock);
    }

    private static Matcher<MockedException> attribute(final Matcher<? super Object> attributeMatcher) {
        return hasFeature("attribute", x -> x.attribute, attributeMatcher);
    }

    private static void assertAndThrowError(final TestSubscriber<?> subscriber) {
        subscriber.assertNoValues();

        final List<Throwable> onErrorEvents = subscriber.getOnErrorEvents();
        assertThat(onErrorEvents, hasSize(1));

        throw Throwables.propagate(onErrorEvents.get(0));
    }
}
