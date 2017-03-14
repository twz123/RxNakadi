package org.zalando.rxnakadi.http;

import java.util.List;

import org.zalando.rxnakadi.domain.EventType;

import io.reactivex.Completable;

/**
 * Nakadi HTTP operations.
 */
public interface NakadiHttpClient {
    Completable publishEvents(final EventType eventType, final List<?> events);
}
