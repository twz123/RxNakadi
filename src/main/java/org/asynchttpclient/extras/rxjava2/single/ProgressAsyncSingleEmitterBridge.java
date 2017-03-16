package org.asynchttpclient.extras.rxjava2.single;

import static java.util.Objects.requireNonNull;

import org.asynchttpclient.handler.ProgressAsyncHandler;

import io.reactivex.SingleEmitter;

final class ProgressAsyncSingleEmitterBridge<T> extends AbstractProgressSingleEmitterBridge<T> {

    private final ProgressAsyncHandler<? extends T> delegate;

    public ProgressAsyncSingleEmitterBridge(final SingleEmitter<T> emitter,
            final ProgressAsyncHandler<? extends T> delegate) {
        super(emitter);
        this.delegate = requireNonNull(delegate);
    }

    @Override
    protected ProgressAsyncHandler<? extends T> delegate() {
        return delegate;
    }

}
