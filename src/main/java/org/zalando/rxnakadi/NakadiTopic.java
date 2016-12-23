package org.zalando.rxnakadi;

import java.util.List;

import org.zalando.rxnakadi.domain.EventBatch;

import rx.Completable;
import rx.Observable;

/**
 * A Nakadi {@literal "topic"} from which events can be consumed and to which events can be published.
 *
 * @param  <E>  the type of events that can be published and consumed
 */
public interface NakadiTopic<E> {

    /**
     * Emits events from this topic.
     */
    Observable<EventBatch<E>> events();

    /**
     * Emits events from this topic, starting at offsets indicated by {@code offsets}.
     *
     * @param   offsets  offsets at which the streaming for a given partition should start.
     *
     * @throws  NullPointerException  if {@code offsets} is {@code null}
     */
    Observable<EventBatch<E>> events(StreamOffsets offsets);

    /**
     * Emits events from this topic using the subscription as described by {@code sd} in {@literal "auto commit"} mode.
     *
     * @param   sd  describes the Nakadi Subscription to be used
     * @param   ac  describes how Nakadi Cursors will be automatically committed
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    Observable<List<E>> events(SubscriptionDescriptor sd, AutoCommit ac);

    /**
     * Publishes events to this topic.
     *
     * @param   events  events to be published
     *
     * @return  a {@code Completable} that completes upon successful publishing, or emits an error.
     *
     * @throws  NullPointerException  if {@code events} is {@code null}
     *
     * @see     NakadiPublishingException
     */
    Completable publish(List<E> events);
}
