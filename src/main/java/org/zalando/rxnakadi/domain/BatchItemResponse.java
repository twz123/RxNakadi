package org.zalando.rxnakadi.domain;

import com.google.common.base.MoreObjects;

/**
 * A status corresponding to one individual event's publishing attempt.
 *
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1789">Nakadi Event
 *       Bus API Definition: #/definitions/BatchItemResponse</a>
 */
public class BatchItemResponse {

    /**
     * @see  #getPublishingStatus()
     */
    public static final String STATUS_SUBMITTED = "submitted";

    /**
     * @see  #getPublishingStatus()
     */
    public static final String STATUS_FAILED = "failed";

    /**
     * @see  #getPublishingStatus()
     */
    public static final String STATUS_ABORTED = "aborted";

    /**
     * @see  #getStep()
     */
    public static final String STEP_NONE = "none";

    /**
     * @see  #getStep()
     */
    public static final String STEP_VALIDATING = "validating";

    /**
     * @see  #getStep()
     */
    public static final String STEP_PARTITIONING = "partitioning";

    /**
     * @see  #getStep()
     */
    public static final String STEP_ENRICHING = "enriching";

    /**
     * @see  #getStep()
     */
    public static final String STEP_PUBLISHING = "publishing";

    private String eid;
    private String publishingStatus;
    private String step;
    private String detail;

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)                      //
                       .omitNullValues()                          //
                       .add("eid", eid)                           //
                       .add("publishingStatus", publishingStatus) //
                       .add("step", step)                         //
                       .add("detail", detail)                     //
                       .toString();
    }

    /**
     * Returns the {@link Metadata#getEid() eid} of the event this item corresponds to.
     *
     * @return  eid of the corresponding event or {@code null} if missing on the incoming event
     */
    public String getEid() {
        return eid;
    }

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
    public String getPublishingStatus() {
        return publishingStatus;
    }

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
    public String getStep() {
        return step;
    }

    /**
     * Human readable information about the failure on this item. Items that are not {@value #STATUS_SUBMITTED} should
     * have a description.
     *
     * @return  the description
     */
    public String getDetail() {
        return detail;
    }
}
