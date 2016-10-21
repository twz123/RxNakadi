package org.zalando.rxnakadi;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Nakadi event type.
 */
public interface EventType {

    /**
     * @return  the name of this event type
     */
    @Override
    String toString();

    static EventType of(final String eventType) {
        checkArgument(eventType.length() > 0, "event type may not be empty");

        return new EventType() {
            @Override
            public String toString() {
                return eventType;
            }
        };
    }
}
