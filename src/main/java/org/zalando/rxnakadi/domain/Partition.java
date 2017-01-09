package org.zalando.rxnakadi.domain;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.immutables.value.Value;

/**
 * Partition information. Can be helpful when trying to start a stream using the unmanaged API. This information is not
 * related to the state of the consumer clients.
 *
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1330">Nakadi Event
 *       Bus API Definition: #/definitions/Partition</a>
 */
@Value.Immutable
public interface Partition {

    @NotNull
    @Size(min = 1)
    String getPartition();

    /**
     * An offset of the oldest available Event in that partition. This value will be changing upon removal of events
     * from the partition by the background archiving/cleanup mechanism.
     */
    @NotNull
    @Size(min = 1)
    String getOldestAvailableOffset();

    /**
     * An offset of the newest available Event in that partition. This value will be changing upon reception of new
     * events for this partition by Nakadi.
     */
    @NotNull
    @Size(min = 1)
    String getNewestAvailableOffset();
}
