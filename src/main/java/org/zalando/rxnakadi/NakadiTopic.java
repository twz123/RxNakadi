package org.zalando.rxnakadi;

import java.util.List;

import rx.Completable;
import rx.Observable;

/**
 * A Nakadi {@literal "topic"} from which events can be consumed and to which events can be published.
 *
 * @param  <E>  the type of events that can be published and consumed
 */
public interface NakadiTopic<E> {

    /**
     * Consumes events from this topic using the subscription as described by {@code sd}.
     *
     * @param  sd  describes the Nakadi Subscription to be used
     * @param  ac  describes how Nakadi Cursors will be automatically committed
     */
    Observable<List<E>> events(SubscriptionDescriptor sd, AutoCommit ac);

    /**
     * Publishes events to this topic.
     *
     * @param   events  events to be published
     *
     * @return  a {@code Completable} that completes upon sucessful publishing, or emits an error.
     *
     * @see     NakadiPublishingException
     */
    Completable publish(List<E> events);
}
