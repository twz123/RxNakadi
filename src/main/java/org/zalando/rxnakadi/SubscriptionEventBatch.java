package org.zalando.rxnakadi;

import org.zalando.rxnakadi.domain.EventBatch;

import rx.Completable;

/**
 * Analogue to {@link EventBatch} but used for streaming events via the managed API, i.e. a
 * {@code SubscriptionEventBatch} can be {@link #commit() committed}.
 *
 * @param  <E>  type of events in this batch
 *
 * @see    <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1437">Nakadi Event
 *         Bus API Definition: #/definitions/SubscriptionEventStreamBatch</a>
 */
public interface SubscriptionEventBatch<E> extends EventBatch<E> {

    /**
     * Commits this {@code SubscriptionEventBatch} and returns a {@code Completable} that may be subscribed to to
     * determine the outcome of the operation.
     *
     * @return  a {@code Completable} indicating the outcome of the commit operation
     */
    Completable commit();

}
