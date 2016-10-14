package org.zalando.nakadilib.utils;

import static com.google.common.base.Preconditions.checkArgument;

import com.netflix.hystrix.exception.HystrixRuntimeException;

/**
 * Strategy, which attempts the action it is attached to for a configurable amount of times until it is evaluated as a
 * failure.
 */
public class FixedAttemptsStrategy {

    /**
     * Maximum amount of attempts.
     */
    private final int maximumAttempts;

    /**
     * Creates a new strategy, which attempts the action it is attached to for the specified amount of times.
     *
     * @param   maximumAttempts  maximum amount of attempts
     *
     * @throws  IllegalArgumentException  if {@code maximumAttempts} is less than one
     */
    public FixedAttemptsStrategy(final int maximumAttempts) {
        checkArgument(maximumAttempts > 0, "Maximum amount of attempts must be greater than zero.");

        this.maximumAttempts = maximumAttempts;
    }

    public boolean shouldBeRetried(final int currentAttempt, final Throwable error) {
        if (currentAttempt >= maximumAttempts) {
            return false;
        }

        if (error instanceof HystrixRuntimeException) {
            switch (((HystrixRuntimeException) error).getFailureType()) {

                case COMMAND_EXCEPTION :
                case TIMEOUT :
                    return true;
            }
        }

        return false;
    }
}
