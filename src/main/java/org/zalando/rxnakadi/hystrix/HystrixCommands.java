package org.zalando.rxnakadi.hystrix;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;

import java.text.MessageFormat;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.hystrix.HystrixInvokableInfo;
import com.netflix.hystrix.HystrixObservable;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

import rx.Observable;
import rx.Single;

public final class HystrixCommands {

    @SuppressWarnings("serial")
    private static final class CommandFailedExcepion extends Exception {
        CommandFailedExcepion(final HystrixInvokableInfo<?> info, final Throwable error) {
            super(MessageFormat.format("{0}.{1}", info.getCommandGroup().name(), info.getCommandKey().name()), error);
        }

        public String getCommandName() {
            return super.getMessage();
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(HystrixCommands.class);

    /**
     * Wraps a Hystrix guarded command into a {@code Single} that may be subscribed to multiple times and unwraps
     * Hystrix exceptions by propagating their cause.
     *
     * @param   commandFactory  source for the Hystrix command to be executed. Every Hystrix command may only be used
     *                          once, hence it's necessary to build a new command for each subscription.
     *
     * @throws  NullPointerException  if {@code commandFactory} is {@code null}
     */
    public static <T> Single<T> toSingle(final Callable<? extends HystrixObservable<T>> commandFactory) {
        return Single.fromCallable(requireNonNull(commandFactory))          //
                     .flatMap(command -> command.toObservable().toSingle()) //
                     .onErrorResumeNext(error -> Single.error(unwrapHystrixExceptions(error)));
    }

    /**
     * Wraps a Hystrix guarded command into a {@code Single} that directly retries the command if it has been terminated
     * due to a {@link HystrixRuntimeException.FailureType#TIMEOUT timeout} or a
     * {@link HystrixRuntimeException.FailureType#COMMAND_EXCEPTION command exception}. Upon terminal failure, Hystrix
     * exceptions are automatically unwrapped and their cause is being propagated.
     *
     * @param   commandFactory  source for the Hystrix command to be executed. Every Hystrix command may only be used
     *                          once, hence it's necessary to build a new command for each attempt.
     * @param   maxAttempts     number of attempts until command failures are propagated
     *
     * @throws  NullPointerException      if {@code commandFactory} is {@code null}
     * @throws  IllegalArgumentException  if {@code maxAttempts} is non-positive
     */
    public static <T, C extends HystrixObservable<T> & HystrixInvokableInfo<?>> Single<T> withRetries(
            final Callable<? extends C> commandFactory, final int maxAttempts) {

        requireNonNull(commandFactory);
        checkArgument(maxAttempts > 0, "maxAttempts must be positive: %s", maxAttempts);

        if (maxAttempts == 1) {
            return toSingle(commandFactory);
        }

        return Observable.fromCallable(requireNonNull(commandFactory))                              //
                         .flatMap(command ->
                                 command.toObservable().onErrorResumeNext(error ->
                                         Observable.error(new CommandFailedExcepion(command, error)))) //
                         .retry((attempt, error) -> shouldBeRetried(error, maxAttempts, attempt))   //
                         .onErrorResumeNext(error ->
                                 Observable.error(unwrapHystrixExceptions(unwrapCommandFailedException(error)))) //
                         .toSingle();
    }

    private static boolean shouldBeRetried(final Throwable error, final int maxAttempts, final int attempt) {

        if (error instanceof CommandFailedExcepion) {
            final CommandFailedExcepion failure = (CommandFailedExcepion) error;

            final Throwable cause = error.getCause();

            if (cause instanceof HystrixRuntimeException) {
                if (attempt < maxAttempts) {
                    switch (((HystrixRuntimeException) cause).getFailureType()) {

                        case COMMAND_EXCEPTION :
                        case TIMEOUT :
                            LOG.warn("Retrying [{}] after attempt [{}]: [{}]", failure.getCommandName(), attempt,
                                firstNonNull(cause.getCause(), cause).toString());
                            return true;
                    }
                }
            }

            LOG.warn("Not retrying [{}] after attempt [{}]: [{}]", failure.getCommandName(), attempt,
                firstNonNull(cause.getCause(), cause).toString());
            return false;
        }

        LOG.warn("Not retrying after attempt [{}]: [{}]", attempt, error.toString());
        return false;
    }

    private static Throwable unwrapHystrixExceptions(final Throwable error) {
        return error instanceof HystrixBadRequestException || error instanceof HystrixRuntimeException
            ? firstNonNull(error.getCause(), error) : error;
    }

    private static Throwable unwrapCommandFailedException(final Throwable error) {
        return error instanceof CommandFailedExcepion ? firstNonNull(error.getCause(), error) : error;
    }

    private HystrixCommands() {
        throw new AssertionError("No instances for you!");
    }
}
