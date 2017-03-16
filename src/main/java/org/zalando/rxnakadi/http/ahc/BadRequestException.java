package org.zalando.rxnakadi.http.ahc;

/**
 * Indicates that provided arguments or state caused the error in contrast to an execution failure.
 *
 * <p>This is meant to not trigger a fallback, not count against failure metrics and thus not trigger the circuit
 * breaker.</p>
 *
 * NOTE: This should <strong>only</strong> be used when an error is due to user input such as
 * {@link IllegalArgumentException} otherwise it defeats the purpose of fault-tolerance and fallback behavior.
 */
public class BadRequestException extends RuntimeException {

    public BadRequestException(final String message) {
        super(message);
    }

    public BadRequestException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
