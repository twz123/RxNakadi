package org.asynchttpclient.extras.rxjava2.single;

import static java.util.Objects.requireNonNull;

import org.asynchttpclient.AsyncHandler;

import io.reactivex.SingleEmitter;

final class AsyncSingleEmitterBridge<T> extends AbstractSingleEmitterBridge<T> {

    private final AsyncHandler<? extends T> delegate;

    public AsyncSingleEmitterBridge(final SingleEmitter<T> emitter, final AsyncHandler<? extends T> delegate) {
        super(emitter);
        this.delegate = requireNonNull(delegate);
    }

    @Override
    protected AsyncHandler<? extends T> delegate() {
        return delegate;
    }

}
