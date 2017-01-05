package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.zalando.rxnakadi.domain.BatchItemResponse;

import com.google.common.collect.ImmutableList;

/**
 * Indicates that some events could not be published to Nakadi.
 *
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L360">Nakadi Event
 *       Bus API Definition: &quot;Batch partially submitted.&quot;</a>
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L371">Nakadi Event
 *       Bus API Definition: &quot;Batch not submitted.&quot;</a>
 */
@SuppressWarnings("serial")
public class NakadiPublishingException extends RuntimeException {

    private final EventType eventType;
    private final String flowId;
    private final List<BatchItemResponse> problems;

    public NakadiPublishingException(final EventType eventType, final String flowId,
            final List<BatchItemResponse> problems) {
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

    public List<BatchItemResponse> getProblems() {
        return problems;
    }
}
