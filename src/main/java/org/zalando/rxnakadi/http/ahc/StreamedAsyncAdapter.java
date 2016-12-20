package org.zalando.rxnakadi.http.ahc;

import static java.util.Objects.requireNonNull;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;

import org.asynchttpclient.handler.StreamedAsyncHandler;

import org.reactivestreams.Publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpHeaders;

/**
 * Simplifies the usage of Reactive Streams along with Async HTTP Client. This handler just collects the HTTP response
 * status and the response headers, and then passes them to its {@link Target}, along with the Publisher that emits the
 * response body chunks.
 */
final class StreamedAsyncAdapter implements StreamedAsyncHandler<Void> {

    interface Target {

        /**
         * Indicates if the Target has been canceled asynchronously and HTTP response processing should be aborted.
         */
        boolean isCanceled();

        /**
         * Notifies the Target that the HTTP response processing has been aborted due to an an error condition.
         *
         * @param  e  the exception encountered during HTTP response processing
         */
        void abort(Throwable e);

        /**
         * Starts the actual streaming of the body payload.
         *
         * @param   responseStatus   HTTP response status that has been received
         * @param   responseHeaders  HTTP respones headers that have been received
         * @param   publisher        the Reactive Streams Publisher that emits the HTTP response payload in chunks
         *
         * @return  {@code true} if request processing shall continue, {@code false} otherwise.
         */
        boolean startBodyStream(                                                    //
                HttpResponseStatus responseStatus,                                  //
                HttpHeaders responseHeaders,                                        //
                Publisher<HttpResponseBodyPart> publisher);
    }

    private static final Logger LOG = LoggerFactory.getLogger(StreamedAsyncAdapter.class);

    private final Target target;

    private HttpResponseStatus responseStatus;
    private HttpHeaders responseHeaders;

    private StreamedAsyncAdapter(final Target target) {
        this.target = requireNonNull(target);
    }

    static AsyncHandler<?> withTarget(final Target target) {
        return new StreamedAsyncAdapter(target);
    }

    @Override
    public State onStatusReceived(final HttpResponseStatus responseStatus) {
        if (target.isCanceled()) {
            return State.ABORT;
        }

        this.responseStatus = responseStatus;
        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(final HttpResponseHeaders headers) {
        if (target.isCanceled()) {
            return State.ABORT;
        }

        if (!headers.isTrailling()) {
            responseHeaders = headers.getHeaders();
        }

        return State.CONTINUE;
    }

    @Override
    public void onThrowable(final Throwable t) {
        abort(t);
    }

    @Override
    public State onStream(final Publisher<HttpResponseBodyPart> publisher) {
        if (target.isCanceled()) {
            return State.ABORT;
        }

        final boolean proceed = target.startBodyStream(responseStatus, responseHeaders, publisher);

        // clean up, not needed anymore
        responseStatus = null;
        responseHeaders = null;

        return proceed ? State.CONTINUE : State.ABORT;
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart bodyPart) {
        LOG.warn("Body part received, but streaming is used.");
        return target.isCanceled() ? State.ABORT : State.CONTINUE;
    }

    @Override
    public Void onCompleted() {
        return null; // Completion will be signaled by the publisher, nothing to do here.
    }

    private State abort(final Throwable error) {
        if (!target.isCanceled()) {
            target.abort(error);
        }

        return State.ABORT;
    }
}
