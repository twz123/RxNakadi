package org.zalando.rxnakadi.domain;

import javax.annotation.Nullable;

import javax.validation.constraints.NotNull;

import org.immutables.value.Value;

/**
 * A status corresponding to one individual event's publishing attempt.
 *
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1789">Nakadi Event
 *       Bus API Definition: #/definitions/BatchItemResponse</a>
 */
@Value.Immutable
public interface BatchItemResponse {

    /**
     * @see  #getPublishingStatus()
     */
    String STATUS_SUBMITTED = "submitted";

    /**
     * @see  #getPublishingStatus()
     */
    String STATUS_FAILED = "failed";

    /**
     * @see  #getPublishingStatus()
     */
    String STATUS_ABORTED = "aborted";

    /**
     * @see  #getStep()
     */
    String STEP_NONE = "none";

    /**
     * @see  #getStep()
     */
    String STEP_VALIDATING = "validating";

    /**
     * @see  #getStep()
     */
    String STEP_PARTITIONING = "partitioning";

    /**
     * @see  #getStep()
     */
    String STEP_ENRICHING = "enriching";

    /**
     * @see  #getStep()
     */
    String STEP_PUBLISHING = "publishing";

    /**
     * Returns the {@link Metadata#getEid() eid} of the event this item corresponds to.
     *
     * @return  eid of the corresponding event or {@code null} if missing on the incoming event
     */
    @Nullable
    String getEid();

    /**
     * Returns the indicator of the submission of this item:
     *
     * <dl>
     *   <dt>{@value #STATUS_SUBMITTED}</dt>
     *   <dd>Indicates successful submission, including commit on the underlying broker.</dd>
     *
     *   <dt>{@value #STATUS_FAILED}</dt>
     *   <dd>Indicates the message submission was not possible and can be resubmitted if so desired.</dd>
     *
     *   <dt>{@value #STATUS_ABORTED}</dt>
     *   <dd>Indicates that the submission of this item was not attempted any further due to a failure on another item
     *     in the batch.</dd>
     * </dl>
     *
     * @return  the publishing status
     */
    @NotNull
    String getPublishingStatus();

    /**
     * Returns the indicator of the step in the publishing process this event reached. In events that
     * {@value #STATUS_FAILED} means the step of the failure.
     *
     * <dl>
     *   <dt>{@value #STEP_NONE}</dt>
     *   <dd>indicates that nothing was yet attempted for the publishing of this event. Should be present only in the
     *     case of aborting the publishing during the validation of another (previous) event.</dd>
     *
     *   <dt>{@value #STEP_VALIDATING}, {@value #STEP_PARTITIONING}, {@value #STEP_ENRICHING} and
     *     {@value #STEP_PUBLISHING}</dt>
     *   <dd>Indicate the corresponding steps of the publishing process.</dd>
     * </dl>
     *
     * @return  the step
     */
    @Nullable
    String getStep();

    /**
     * Human readable information about the failure on this item. Items that are not {@value #STATUS_SUBMITTED} should
     * have a description.
     *
     * @return  the description
     */
    @Nullable
    String getDetail();

}
