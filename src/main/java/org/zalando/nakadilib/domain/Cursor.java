package org.zalando.nakadilib.domain;

import com.google.common.base.MoreObjects;

public class Cursor {

    /**
     * Id of the partition pointed to by this cursor.
     */
    private String partition;

    /**
     * Offset of the event being pointed to.
     */
    private long offset;

    /**
     * An opaque value defined by the server.
     */
    private String cursorToken;

    /**
     * The name of the event type this partition's events belong to.
     */
    private String eventType;

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)            //
                       .add("partition", partition)     //
                       .add("offset", offset)           //
                       .add("cursorToken", cursorToken) //
                       .add("eventType", eventType)     //
                       .toString();
    }
}
