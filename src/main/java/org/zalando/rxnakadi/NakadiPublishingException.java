package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.zalando.rxnakadi.domain.PublishingProblem;

import com.google.common.collect.ImmutableList;

/**
 * Indicates that some events could not be published to Nakadi.
 */
@SuppressWarnings("serial")
public class NakadiPublishingException extends RuntimeException {

    private final EventType eventType;
    private final List<PublishingProblem> problems;

    public NakadiPublishingException(final EventType eventType, final List<PublishingProblem> problems) {
        this.eventType = requireNonNull(eventType);
        this.problems = ImmutableList.copyOf(problems);
    }

    @Override
    public String getMessage() {
        return problems.size() + " events of type " + eventType + " could not be published to Nakadi.";
    }

    public EventType getEventType() {
        return eventType;
    }

    public List<PublishingProblem> getProblems() {
        return problems;
    }
}
