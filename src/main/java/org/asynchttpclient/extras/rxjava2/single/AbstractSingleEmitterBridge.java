package org.asynchttpclient.extras.rxjava2.single;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;

import org.asynchttpclient.extras.rxjava2.AbortedException;
import org.asynchttpclient.extras.rxjava2.UnsubscribedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.SingleEmitter;

import io.reactivex.exceptions.CompositeException;
import io.reactivex.exceptions.Exceptions;

public abstract class AbstractSingleEmitterBridge<T> implements AsyncHandler<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSingleEmitterBridge.class);

    protected final SingleEmitter<T> emitter;

    private final AtomicBoolean delegateTerminated = new AtomicBoolean();

    protected AbstractSingleEmitterBridge(final SingleEmitter<T> emitter) {
        this.emitter = requireNonNull(emitter);
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart content) throws Exception {
        return emitter.isDisposed() ? abort() : delegate().onBodyPartReceived(content);
    }

    @Override
    public State onStatusReceived(final HttpResponseStatus status) throws Exception {
        return emitter.isDisposed() ? abort() : delegate().onStatusReceived(status);
    }

    @Override
    public State onHeadersReceived(final HttpResponseHeaders headers) throws Exception {
        return emitter.isDisposed() ? abort() : delegate().onHeadersReceived(headers);
    }

    @Override
    public Void onCompleted() {
        if (delegateTerminated.getAndSet(true)) {
            return null;
        }

        final T result;
        try {
            result = delegate().onCompleted();
        } catch (final Throwable t) {
            emitOnError(t);
            return null;
        }

        if (!emitter.isDisposed()) {
            if (result == null) {
                emitter.onError(new AbortedException());
            } else {
                emitter.onSuccess(result);
            }
        }

        return null;
    }

    @Override
    public void onThrowable(final Throwable t) {
        if (delegateTerminated.getAndSet(true)) {
            return;
        }

        Throwable error = t;
        try {
            delegate().onThrowable(t);
        } catch (final Throwable x) {
            error = new CompositeException(Arrays.asList(t, x));
        }

        emitOnError(error);
    }

    protected State abort() {
        if (!delegateTerminated.getAndSet(true)) {

            // send a terminal event to the delegate
            // e.g. to trigger cleanup logic
            delegate().onThrowable(new UnsubscribedException());
        }

        return State.ABORT;
    }

    protected abstract AsyncHandler<? extends T> delegate();

    private void emitOnError(final Throwable error) {
        Exceptions.throwIfFatal(error);
        if (!emitter.isDisposed()) {
            emitter.onError(error);
        } else {
            LOGGER.debug("Not propagating onError after unsubscription: {}", error.getMessage(), error);
        }
    }
}
