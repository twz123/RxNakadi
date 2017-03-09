package org.asynchttpclient.extras.rxjava2.single;

import org.asynchttpclient.handler.ProgressAsyncHandler;

import io.reactivex.SingleEmitter;

public abstract class AbstractProgressSingleEmitterBridge<T> extends AbstractSingleEmitterBridge<T>
    implements ProgressAsyncHandler<Void> {
    protected AbstractProgressSingleEmitterBridge(final SingleEmitter<T> emitter) {
        super(emitter);
    }

    @Override
    public State onHeadersWritten() {
        return emitter.isDisposed() ? abort() : delegate().onHeadersWritten();
    }

    @Override
    public State onContentWritten() {
        return emitter.isDisposed() ? abort() : delegate().onContentWritten();
    }

    @Override
    public State onContentWriteProgress(final long amount, final long current, final long total) {
        return emitter.isDisposed() ? abort() : delegate().onContentWriteProgress(amount, current, total);
    }

    @Override
    protected abstract ProgressAsyncHandler<? extends T> delegate();

}
