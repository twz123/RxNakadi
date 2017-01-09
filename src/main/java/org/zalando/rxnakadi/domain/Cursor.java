package org.zalando.rxnakadi.domain;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.immutables.value.Value;

/**
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1366">Nakadi Event
 *       Bus API Definition: #/definitions/Cursor</a>
 */
@Value.Immutable
public interface Cursor {

    /**
     * Id of the partition pointed to by this cursor.
     */
    @NotNull
    @Size(min = 1)
    String getPartition();

    /**
     * Offset of the event being pointed to.
     */
    @NotNull
    @Size(min = 1)
    String getOffset();

}
