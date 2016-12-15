package org.zalando.rxnakadi.domain;

import java.util.List;

import com.google.common.base.MoreObjects;

/**
 * @param  <E>  Nakadi event type contained in this batch
 */
public class EventBatch<E> {

    private final Object cursor;
    private final List<E> events;

    public EventBatch(final Object cursor, final List<E> events) {
        this.cursor = cursor;
        this.events = events;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)  //
                          .add("cursor", cursor) //
                          .add("events", events) //
                          .toString();
    }

    public Object getCursor() {
        return cursor;
    }

    public List<E> getEvents() {
        return events;
    }
}
