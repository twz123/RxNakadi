package org.zalando.rxnakadi.http;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.zalando.rxnakadi.EventType;
import org.zalando.rxnakadi.NakadiSubscription;

import rx.Completable;
import rx.Observable;
import rx.Single;

/**
 * Nakadi HTTP operations.
 */
public interface NakadiHttpClient {

    Single<NakadiSubscription> getSubscription( //
            final EventType eventType, final String owningApplication, final String consumerGroup);

    Observable<String> getEventsForType(final EventType eventType);

    Observable<String> getEventsForSubscription(final String subscriptionId, final Consumer<String> streamId);

    Completable commitCursor(final Optional<Object> cursor, final String subscriptionId, final String streamId);

    Completable publishEvents(final EventType eventType, final List<?> events);
}
