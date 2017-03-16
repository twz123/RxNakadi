package org.asynchttpclient.extras.rxjava2;

import java.util.concurrent.CancellationException;

/**
 * Indicates that an {@code Observer} unsubscribed during the processing of a HTTP request.
 */
@SuppressWarnings("serial")
public class UnsubscribedException extends CancellationException {

    public UnsubscribedException() { }

    public UnsubscribedException(final Throwable cause) {
        initCause(cause);
    }
}
