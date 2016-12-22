package org.zalando.rxnakadi;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;

/**
 * A Nakadi event type.
 *
 * @see  <a href="https://github.com/zalando/nakadi/#events-and-event-types">API Overview and Usage: Events and Event
 *       Types</a>
 */
public final class EventType {

    private final String eventType;

    private EventType(final String eventType) {
        checkArgument(eventType.length() > 0, "event type may not be empty");
        this.eventType = eventType;
    }

    public static EventType of(final String eventType) {
        return new EventType(eventType);
    }

    /**
     * @return  the name of this event type
     */
    @Override
    public String toString() {
        return eventType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(EventType.class, eventType);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj == this || (obj instanceof EventType && eventType.equals(((EventType) obj).eventType));
    }
}
