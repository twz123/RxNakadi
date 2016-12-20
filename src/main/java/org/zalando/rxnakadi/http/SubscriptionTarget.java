package org.zalando.rxnakadi.http;

import static java.util.Objects.requireNonNull;

import rx.Subscriber;

/**
 * Abstract bridge between a {@link StreamedAsyncAdapter.Target Streamed Target} and an RxJava {@link Subscriber}.
 *
 * @param  <T>  the type of items the wrapped Subscriber expects to observe
 */
abstract class SubscriptionTarget<T> implements StreamedAsyncAdapter.Target {

    protected final Subscriber<? super T> subscriber;

    protected SubscriptionTarget(final Subscriber<? super T> subscriber) {
        this.subscriber = requireNonNull(subscriber);
    }

    @Override
    public boolean isCanceled() {
        return subscriber.isUnsubscribed();
    }

    @Override
    public void abort(final Throwable e) {
        subscriber.onError(e);
    }
}
