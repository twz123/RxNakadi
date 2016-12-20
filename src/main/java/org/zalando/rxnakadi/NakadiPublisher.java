package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.inject.Inject;

import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.http.NakadiHttpClient;

import rx.Completable;

/**
 * Write events into the Nakadi event bus.
 */
public final class NakadiPublisher {

    private final NakadiHttpClient http;

    @Inject
    NakadiPublisher(final NakadiHttpClient http) {
        this.http = requireNonNull(http);
    }

    public <E extends NakadiEvent> Completable publishEvents(final EventType eventType, final List<E> events) {
        return http.publishEvents(eventType, events);
    }

}
