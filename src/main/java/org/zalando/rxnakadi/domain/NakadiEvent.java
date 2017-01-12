package org.zalando.rxnakadi.domain;

import java.time.Instant;

import java.util.function.Supplier;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Base interface for Nakadi events.
 */
public interface NakadiEvent {

    static <E extends NakadiEvent> E create(final Supplier<E> ctor) {
        return create(ctor, Instant.now());
    }

    static <E extends NakadiEvent> E create(final Supplier<E> ctor, final Instant occurredAt) {
        final E event = ctor.get();
        event.withMetadata(Metadata.create(occurredAt));
        return event;
    }

    @NotNull
    @Valid
    Metadata getMetadata();

    /**
     * Returns a Nakadi event that carries the exact same data as this event, but with the provided {@code metadata}.
     *
     * <p>The returned event may be a copy of the event, or the same, modified event. This implementation detail is up
     * to implementors.</p>
     *
     * <p>The returned event needs to be {@link Class#isInstance(Object) assignment-compatible} to this event's runtime
     * type, i.e. {@code event.getClass().isInstance(event.withMetadata(metadata))} is required to be {@code true}.</p>
     *
     * @param   metadata  the {@code Metadata} for the returned event
     *
     * @return  a {@code NakadiEvent} with the specified metadata.
     */
    NakadiEvent withMetadata(Metadata metadata);

}
