package org.zalando.rxnakadi.domain;

import java.time.Instant;

import java.util.UUID;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.google.common.base.MoreObjects;

public class Metadata {

    @NotNull
    @Size(min = 1)
    private String eid;

    @NotNull
    private Instant occurredAt;

    private Instant receivedAt;

    public static Metadata create() {
        return create(Instant.now());
    }

    public static Metadata create(final Instant occurredAt) {
        final Metadata metadata = new Metadata();
        metadata.setEid(UUID.randomUUID().toString());
        metadata.setOccurredAt(occurredAt);
        return metadata;
    }

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)          //
                       .omitNullValues()              //
                       .add("eid", eid)               //
                       .add("occurredAt", occurredAt) //
                       .add("receivedAt", receivedAt) //
                       .toString();
    }

    public String getEid() {
        return eid;
    }

    public void setEid(final String eid) {
        this.eid = eid;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(final Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(final Instant receivedAt) {
        this.receivedAt = receivedAt;
    }
}
