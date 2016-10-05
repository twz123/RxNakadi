package org.zalando.nakadilib.domain;

import java.time.Instant;

import java.util.function.Supplier;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class NakadiEvent {

    @NotNull
    @Valid
    private Metadata metadata;

    protected static <E extends NakadiEvent> E create(final Supplier<E> ctor) {
        return create(ctor, Instant.now());
    }

    protected static <E extends NakadiEvent> E create(final Supplier<E> ctor, final Instant occurredAt) {
        final E event = ctor.get();
        event.setMetadata(Metadata.create(occurredAt));
        return event;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(final Metadata metadata) {
        this.metadata = metadata;
    }
}
