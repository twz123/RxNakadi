package org.asynchttpclient.extras.rxjava2;

import java.util.concurrent.CancellationException;

/**
 * Indicates that the upstream ahc handler aborted the request.
 */
@SuppressWarnings("serial")
public class AbortedException extends CancellationException {

    public AbortedException() { }

    public AbortedException(final Throwable cause) {
        initCause(cause);
    }
}
