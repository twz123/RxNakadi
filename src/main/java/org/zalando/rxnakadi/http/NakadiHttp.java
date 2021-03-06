package org.zalando.rxnakadi.http;

import java.util.regex.Pattern;

import org.zalando.rxnakadi.domain.Cursor;

import com.google.common.net.MediaType;

/**
 * Various constants used when communicating to Nakadi via HTTP.
 */
public final class NakadiHttp {

    /**
     * The HTTP header specifying the {@literal "Flow ID"} of the request, which is written into the logs and passed to
     * called services. Helpful for operational troubleshooting and log analysis.
     */
    public static final String X_FLOW_ID = "X-Flow-Id";

    /**
     * The HTTP header specifying the ID of the stream which the client uses to read events. It is not possible to make
     * a commit for a terminated or none-existing stream. Also the client can't commit something which was not sent to
     * his stream.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L822">Notion of
     *       this header in the Nakadi Event Bus API Definition</a>
     */
    public static final String X_NAKADI_STREAM_ID = "X-Nakadi-StreamId";

    /**
     * The HTTP header specifying the {@link Cursor cursors} indicating the partitions to read from and respective
     * starting offsets.
     *
     * <p>Assumes the offset on each cursor is not inclusive (i.e., first delivered Event is the <strong>first one
     * after</strong> the one pointed to in the cursor).</p>
     *
     * <p>If the header is not present, the stream for all partitions defined for the EventType will start from the
     * newest event available in the system at the moment of making this call.
     *
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L418">Notion of
     *       this header in the Nakadi Event Bus API Definition</a>
     */
    public static final String X_NAKADI_CURSORS = "X-Nakadi-Cursors";

    /**
     * Media Type {@literal "application/json"}.
     */
    public static final MediaType JSON_TYPE = MediaType.JSON_UTF_8.withoutParameters();

    /**
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1881">Nakadi
     *       Event Bus API Definition: #/parameters/BatchLimit</a>
     */
    public static final String PARAM_BATCH_LIMIT = "batch_limit";

    /**
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1894">Nakadi
     *       Event Bus API Definition: #/parameters/StreamLimit</a>
     */
    public static final String PARAM_STREAM_LIMIT = "stream_limit";

    /**
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1909">Nakadi
     *       Event Bus API Definition: #/parameters/BatchFlushTimeout</a>
     */
    public static final String PARAM_BATCH_FLUSH_TIMEOUT = "batch_flush_timeout";

    /**
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1924">Nakadi
     *       Event Bus API Definition: #/parameters/StreamTimeout</a>
     */
    public static final String PARAM_STREAM_TIMEOUT = "stream_timeout";

    /**
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1948">Nakadi
     *       Event Bus API Definition: #/parameters/StreamKeepAliveLimit</a>
     */
    public static final String PARAM_STREAM_KEEP_ALIVE_LIMIT = "stream_keep_alive_limit";

    /**
     * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L884">Notion of
     *       <code>max_uncommitted_events</code> in the Nakadi Event Bus API Definition</a>
     */
    public static final String PARAM_MAX_UNCOMMITTED_EVENTS = "max_uncommitted_events";

    /**
     * Used to split the character stream into individual JSON chunks of Nakadi's flavor of the
     * {@literal "application/x-json-stream"} Media Type. According to the Nakadi Event Bus API Definition, this is
     * always the newline character (see <a
     * href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L456">here</a> and <a
     * href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L908">here</a>).
     */
    public static final Pattern EVENTS_DELIMITER_PATTERN = Pattern.compile("\n", Pattern.LITERAL);

    private NakadiHttp() {
        throw new AssertionError("No instances for you!");
    }
}
