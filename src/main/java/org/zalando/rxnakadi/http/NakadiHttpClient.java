package org.zalando.rxnakadi.http;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.zalando.rxnakadi.EventType;
import org.zalando.rxnakadi.StreamParameters;
import org.zalando.rxnakadi.SubscriptionDescriptor;
import org.zalando.rxnakadi.domain.Cursor;
import org.zalando.rxnakadi.domain.NakadiSubscription;
import org.zalando.rxnakadi.domain.Partition;

import rx.Completable;
import rx.Observable;
import rx.Single;

/**
 * Nakadi HTTP operations.
 */
public interface NakadiHttpClient {

    Single<NakadiSubscription> getSubscription(final EventType eventType, SubscriptionDescriptor sd);

    Observable<String> getEventsForType(final EventType eventType, StreamParameters params);

    Observable<String> getEventsForType(EventType eventType, StreamParameters params, List<Cursor> cursors);

    Observable<String> getEventsForSubscription(final String subscriptionId, StreamParameters params,
            final Consumer<String> streamId);

    Completable commitCursor(final Optional<Object> cursor, final String subscriptionId, final String streamId);

    Completable publishEvents(final EventType eventType, final List<?> events);

    Single<List<Partition>> getPartitions(EventType eventType);

}
