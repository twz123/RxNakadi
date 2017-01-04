package org.zalando.rxnakadi.domain;

import java.util.List;

import com.google.common.base.MoreObjects;

/**
 * @param  <E>  Nakadi event type contained in this batch
 */
public final class EventBatch<E> {

    private Object cursor;
    private List<E> events;
    private Object info;

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)  //
                       .omitNullValues()      //
                       .add("cursor", cursor) //
                       .add("events", events) //
                       .add("info", info)     //
                       .toString();
    }

    public Object getCursor() {
        return cursor;
    }

    public List<E> getEvents() {
        return events;
    }
}
