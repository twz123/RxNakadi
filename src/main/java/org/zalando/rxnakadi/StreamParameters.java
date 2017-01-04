package org.zalando.rxnakadi;

import java.util.OptionalInt;

/**
 * Parameters to be used when opening Nakadi streams.
 *
 * @see  NakadiTopic#events(StreamParameters)
 * @see  NakadiTopic#events(StreamOffsets, StreamParameters)
 * @see  NakadiTopic#events(SubscriptionDescriptor, AutoCommit, StreamParameters)
 */
public interface StreamParameters {

    StreamParameters DEFAULTS = new StreamParameters() {
        @Override
        public String toString() {
            return "StreamParameters.DEFAULTS";
        }
    };

    /**
     * Maximum number of events in each chunk (and therefore per partition) of the stream. If {@code 0} or
     * {@link OptionalInt#isPresent() unspecified}, will buffer events indefinitely and flush on reaching of
     * {@link #batchFlushTimeout() batch flush timeout}.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1880">Nakadi
     *       Event Bus API Definition: #/parameters/BatchLimit</a>
     */
    default OptionalInt batchLimit() {
        return OptionalInt.empty();
    }

    /**
     * Maximum number of events in this stream (over all partitions being streamed in this connection). If {@code 0} or
     * {@link OptionalInt#isPresent() unspecified}, will stream batches indefinitely. Stream initialization will fail if
     * the stream limit is lower than the {@link #batchLimit() batch limit}.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1893">Nakadi
     *       Event Bus API Definition: #/parameters/StreamLimit</a>
     */
    default OptionalInt streamLimit() {
        return OptionalInt.empty();
    }

    /**
     * Maximum time in seconds to wait for the flushing of each chunk (per partition). If the amount of buffered events
     * reaches the {@link #batchLimit() batch limit} before this batch flush timeout is reached, the messages are
     * immediately flushed to the client and batch flush timer is reset. If {@code 0} or
     * {@link OptionalInt#isPresent() unspecified}, will assume 30 seconds.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1908">Nakadi
     *       Event Bus API Definition: #/parameters/BatchFlushTimeout</a>
     */
    default OptionalInt batchFlushTimeout() {
        return OptionalInt.empty();
    }

    /**
     * Maximum time in seconds a stream will live before connection is closed by the server. If {@code 0} or
     * {@link OptionalInt#isPresent() unspecified}, will stream indefinitely. If this timeout is reached, any pending
     * messages (in the sense of {@link #streamLimit() stream limit}) will be flushed to the client. Stream
     * initialization will fail if this stream timeout is lower than the
     * {@link #batchFlushTimeout() batch flush timeout}.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1923">Nakadi
     *       Event Bus API Definition: #/parameters/StreamTimeout</a>
     */
    default OptionalInt streamTimeout() {
        return OptionalInt.empty();
    }

    /**
     * Maximum number of empty keep alive batches to get in a row before closing the connection. If {@code 0} or
     * {@link OptionalInt#isPresent() unspecified}, will send keep alive messages indefinitely.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1947">Nakadi
     *       Event Bus API Definition: #/parameters/StreamKeepAliveLimit</a>
     */
    default OptionalInt streamKeepAliveLimit() {
        return OptionalInt.empty();
    }

    /**
     * Only effective for subscription event streams: The amount of uncommitted events Nakadi will stream before pausing
     * the stream. When in paused state and commit comes - the stream will resume. Minimal value is {@code 1}. Defaults
     * to {@code 10} if not specified.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L884">Notion of
     *       <code>max_uncommitted_events</code> in the Nakadi Event Bus API Definition</a>
     */
    default OptionalInt maxUncommittedEvents() {
        return OptionalInt.empty();
    }
}
