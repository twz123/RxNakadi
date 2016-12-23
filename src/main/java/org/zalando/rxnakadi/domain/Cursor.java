package org.zalando.rxnakadi.domain;

import com.google.common.base.MoreObjects;

/**
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2016_12_08_RC1/api/nakadi-event-bus-api.yaml#L1348">Nakadi
 *       Event Bus API Definition: #/definitions/Cursor</a>
 */
public class Cursor {

    /**
     * Id of the partition pointed to by this cursor.
     */
    private String partition;

    /**
     * Offset of the event being pointed to.
     */
    private String offset;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)        //
                          .add("partition", partition) //
                          .add("offset", offset)       //
                          .toString();
    }

    public String getPartition() {
        return partition;
    }

    public Cursor setPartition(final String partition) {
        this.partition = partition;
        return this;
    }

    public String getOffset() {
        return offset;
    }

    public Cursor setOffset(final String offset) {
        this.offset = offset;
        return this;
    }
}
