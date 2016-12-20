package org.zalando.rxnakadi.http;

import java.lang.reflect.Type;

import java.util.List;
import java.util.regex.Pattern;

import org.zalando.rxnakadi.domain.PublishingProblem;

import com.google.common.net.MediaType;

import com.google.gson.reflect.TypeToken;

import com.netflix.hystrix.HystrixCommandGroupKey;

/**
 * Various constants used when communicating to Nakadi via HTTP.
 */
public final class NakadiHttp {

    /**
     * The HTTP header specifying the identifier to use when committing the cursor.
     */
    public static final String X_NAKADI_STREAM_ID = "X-Nakadi-StreamId";

    /**
     * Media Type {@literal "application/json"}.
     */
    public static final MediaType JSON_TYPE = MediaType.JSON_UTF_8.withoutParameters();

    /**
     * Used to split the character stream into individual JSON chunks of Nakadi's flavor of the
     * {@literal "application/x-json-stream"} Media Type. According to the <a
     * href="https://github.com/zalando/nakadi/blob/R2016_12_08_RC1/api/nakadi-event-bus-api.yaml#L460">Nakadi API
     * specification</a>, this is always the newline character.
     */
    public static final Pattern EVENTS_DELIMITER_PATTERN = Pattern.compile("\n", Pattern.LITERAL);

    public static final Type PUBLISHING_PROBLEM_LIST = new TypeToken<List<PublishingProblem>>() {
            // capture generic type
        }.getType();

    /**
     * Key of the Hystrix command group for all HTTP requests to Nakadi.
     */
    public static final HystrixCommandGroupKey HYSTRIX_GROUP = HystrixCommandGroupKey.Factory.asKey("nakadi");

    private NakadiHttp() {
        throw new AssertionError("No instances for you!");
    }
}
