package org.zalando.rxnakadi.domain;

import java.util.List;

/**
 * @param  <E>  Nakadi event type contained in this batch
 */
public class EventBatch<E> {
    private Object cursor;

    private List<E> events;

    public Object getCursor() {
        return cursor;
    }

    public void setCursor(final Object cursor) {
        this.cursor = cursor;
    }

    public List<E> getEvents() {
        return events;
    }

    public void setEvents(final List<E> events) {
        this.events = events;
    }

}
