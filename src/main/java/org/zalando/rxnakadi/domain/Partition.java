package org.zalando.rxnakadi.domain;

import com.google.common.base.MoreObjects;

/**
 * Partition information. Can be helpful when trying to start a stream using an unmanaged API. This information is not
 * related to the state of the consumer clients.
 *
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2016_12_08_RC1/api/nakadi-event-bus-api.yaml#L1312">Nakadi
 *       Event Bus API Definition: #/definitions/Partition</a>
 */
public class Partition {

    private String partition;

    /**
     * An offset of the oldest available Event in that partition. This value will be changing upon removal of events
     * from the partition by the background archiving/cleanup mechanism.
     */
    private String oldestAvailableOffset;

    /**
     * An offset of the newest available Event in that partition. This value will be changing upon reception of new
     * events for this partition by Nakadi.
     */
    private String newestAvailableOffset;

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)                                //
                       .add("partition", partition)                         //
                       .add("oldestAvailableOffset", oldestAvailableOffset) //
                       .add("newestAvailableOffset", newestAvailableOffset) //
                       .toString();
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(final String partition) {
        this.partition = partition;
    }

    public String getOldestAvailableOffset() {
        return oldestAvailableOffset;
    }

    public void setOldestAvailableOffset(final String oldestAvailableOffset) {
        this.oldestAvailableOffset = oldestAvailableOffset;
    }

    public String getNewestAvailableOffset() {
        return newestAvailableOffset;
    }

    public void setNewestAvailableOffset(final String newestAvailableOffset) {
        this.newestAvailableOffset = newestAvailableOffset;
    }
}
