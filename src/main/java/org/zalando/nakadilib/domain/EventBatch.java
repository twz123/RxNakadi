package org.zalando.nakadilib.domain;

import java.util.List;

/**
 * @param  <E>  Nakadi event type contained in this batch
 */
public class EventBatch<E> {
    private Cursor cursor;

    private List<E> events;

    public Cursor getCursor() {
        return cursor;
    }

    public void setCursor(final Cursor cursor) {
        this.cursor = cursor;
    }

    public List<E> getEvents() {
        return events;
    }

    public void setEvents(final List<E> events) {
        this.events = events;
    }

}
