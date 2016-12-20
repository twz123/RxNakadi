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
    private final String flowId;
    private final List<PublishingProblem> problems;

    public NakadiPublishingException(final EventType eventType, final String flowId,
            final List<PublishingProblem> problems) {
        this.eventType = requireNonNull(eventType);
        this.flowId = flowId;
        this.problems = ImmutableList.copyOf(problems);
    }

    @Override
    public String getMessage() {
        return String.format("%s events of type '%s' could not be published to Nakadi. (Flow-ID: %s)", //
                problems.size(), eventType, flowId);
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getFlowId() {
        return flowId;
    }

    public List<PublishingProblem> getProblems() {
        return problems;
    }
}
