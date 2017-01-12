package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import java.util.Objects;

import org.zalando.rxnakadi.domain.EventType;

import com.google.common.reflect.TypeToken;

/**
 * Describes a Nakadi {@literal "topic"}. This is basically a {@link EventType Nakadi event type} along with the Java
 * type representing individual event instances of that event type.
 *
 * @param  <E>  type of events of the described topic
 */
public final class TopicDescriptor<E> {

    private final EventType eventType;
    private final TypeToken<E> eventTypeToken;

    public static final class Builder<E> {
        final TypeToken<E> eventTypeToken;
        EventType eventType;

        Builder(final TypeToken<E> eventTypeToken) {
            this.eventTypeToken = requireNonNull(eventTypeToken);
        }

        public Builder<E> from(final EventType eventType) {
            this.eventType = requireNonNull(eventType);
            return this;
        }

        public TopicDescriptor<E> build() {
            checkState(eventType != null, "No event type specified.");
            return new TopicDescriptor<>(this);
        }
    }

    private TopicDescriptor(final Builder<E> builder) {
        this.eventType = builder.eventType;
        this.eventTypeToken = builder.eventTypeToken;
    }

    public static <E> Builder<E> ofType(final Class<E> type) {
        return ofType(TypeToken.of(type));
    }

    public static <E> Builder<E> ofType(final TypeToken<E> type) {
        return new Builder<>(type);
    }

    @Override
    public String toString() {
        return String.format("TopicDescriptor{%s from %s}", eventTypeToken, eventType);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj instanceof TopicDescriptor) {
            final TopicDescriptor<?> other = (TopicDescriptor<?>) obj;
            return eventType.equals(other.eventType) && eventTypeToken.equals(other.eventTypeToken);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventTypeToken);
    }

    public EventType getEventType() {
        return eventType;
    }

    public TypeToken<E> getEventTypeToken() {
        return eventTypeToken;
    }
}
