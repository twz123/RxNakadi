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
     * Consume events from all partitions of this topic, starting at the end of each partition. No special
     * {@link StreamParameters} will be used when opening event streams.
     *
     * @return  an {@code Observable} that emits events from this topic, opening a new stream for each subscription
     */
    default Observable<EventBatch<E>> events() {
        return events(StreamParameters.DEFAULTS);
    }

    /**
     * Consume events from all partitions of this topic, starting at the end of each partition.
     *
     * @param   params  stream parameters to use when opening event streams
     *
     * @return  an {@code Observable} that emits events from this topic, opening a new stream for each subscription
     *
     * @throws  NullPointerException  if {@code params} is {@code null}
     */
    Observable<EventBatch<E>> events(StreamParameters params);

    /**
     * Consume events from this topic, starting at partition offsets indicated by {@code offsets}. No special
     * {@link StreamParameters} will be used when opening event streams.
     *
     * @param   offsets  offsets at which the streaming for a given partition should start
     *
     * @return  an {@code Observable} that emits events from this topic, opening a new stream for each subscription
     *
     * @throws  NullPointerException  if {@code offsets} is {@code null}
     */
    default Observable<EventBatch<E>> events(final StreamOffsets offsets) {
        return events(offsets, StreamParameters.DEFAULTS);
    }

    /**
     * Consume events from this topic, starting at partition offsets indicated by {@code offsets}.
     *
     * @param   offsets  offsets at which the streaming for a given partition should start
     * @param   params   stream parameters to use when opening event streams
     *
     * @return  an {@code Observable} that emits events from this topic, opening a new stream for each subscription
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    Observable<EventBatch<E>> events(StreamOffsets offsets, StreamParameters params);

    /**
     * Consume events from this topic using the subscription as described by {@code sd} in {@literal "auto commit"}
     * mode. No special {@link SubscriptionStreamParameters} will be used when opening event streams.
     *
     * <p>The event streams opened by the returned {@code Observable} will be <em>managed</em>; commits happen
     * automatically as defined by the {@code ac} parameter.</p>
     *
     * @param   sd  describes the Nakadi Subscription to be used
     * @param   ac  describes how Nakadi Cursors will be automatically committed
     *
     * @return  an {@code Observable} that emits events from this topic, opening a new stream for each subscription
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    default Observable<List<E>> events(final SubscriptionDescriptor sd, final AutoCommit ac) {
        return events(sd, ac, StreamParameters.DEFAULTS);
    }

    /**
     * Consume events from this topic using the subscription as described by {@code sd} in {@literal "auto commit"}
     * mode.
     *
     * <p>The event streams opened by the returned {@code Observable} will be <em>managed</em>; commits happen
     * automatically as defined by the {@code ac} parameter.</p>
     *
     * @param   sd      describes the Nakadi Subscription to be used
     * @param   ac      describes how Nakadi Cursors will be automatically committed
     * @param   params  stream parameters to use when opening event streams
     *
     * @return  an {@code Observable} that emits events from this topic, opening a new stream for each subscription
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    Observable<List<E>> events(SubscriptionDescriptor sd, AutoCommit ac, StreamParameters params);

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
