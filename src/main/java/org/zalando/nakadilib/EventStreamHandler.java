package org.zalando.nakadilib;

import static java.util.Objects.requireNonNull;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.MediaType;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import io.netty.handler.codec.http.HttpHeaders;

import io.undertow.util.StatusCodes;

import org.zalando.nakadilib.domain.EventBatch;
import org.zalando.nakadilib.domain.NakadiEvent;
import rx.Single;
import rx.Subscriber;

import rx.subjects.AsyncSubject;

/**
 * Creates the desired Nakadi events by consuming a continuous stream of raw data.
 *
 * <p>Implementation detail: Even if it takes a ReactiveX subscriber, it does not use an {@link rx.Observable}, but it
 * obeys the core tenets formulated for them:</p>
 *
 * <ul>
 *   <li>It checks the Subscriber's {@link Subscriber#isUnsubscribed()} status before emitting any item.</li>
 *   <li>It calls the Subscriber's {@link Subscriber#onNext(Object)} method any number of times and guarantees that
 *     these don't overlap.</li>
 *   <li>It calls either the Subscriber's {@link Subscriber#onCompleted()} or {@link Subscriber#onError(Throwable)}
 *     method, but not both, exactly once, and it does not call the the Subscriber's {@link Subscriber#onNext(Object)}
 *     method subsequently.</li>
 *   <li>Additionally it does not block.</li>
 * </ul>
 *
 * @param  <E>  Nakadi event type being produced
 */
final class EventStreamHandler<E extends NakadiEvent> implements AsyncHandler<EventStreamHandler<E>> {

    /**
     * The HTTP header specifying the identifier to use when committing the cursor.
     */
    public static final String NAKADI_CLIENT_IDENTIFIER_HEADER = "X-Nakadi-StreamId";

    /**
     * Logging instance of this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventStreamHandler.class);

    /**
     * Target, which receives the produced events.
     */
    private final Subscriber<? super EventBatch<E>> subscriber;

    /**
     * Parses payloads.
     */
    private final Function<? super String, ? extends EventBatch<E>> parser;

    /**
     * Identifier of the Nakadi session used for this processor.
     */
    private final AsyncSubject<String> sessionId = AsyncSubject.create();

    /**
     * As it is not guaranteed that every bodypart contains a complete event, a partly received event is stored until it
     * was received completely.
     */
    private final List<HttpResponseBodyPart> bodyParts = new ArrayList<>(2);

    /**
     * Charset being used to decode the endpoints payload.
     */
    private volatile Charset charset;

    private EventStreamHandler(final Subscriber<? super EventBatch<E>> subscriber,
            final Function<? super String, ? extends EventBatch<E>> parser) {
        this.subscriber = requireNonNull(subscriber);
        this.parser = requireNonNull(parser);
    }

    /**
     * Creates a new handler that publishes events of type {@link E} out of raw data.
     *
     * <p>Each valid event is passed to the subscriber for consuming.</p>
     *
     * @param  subscriber  target for the produced events
     * @param  parser      parsing function that converts JSON payload to events
     */
    public static <E extends NakadiEvent> EventStreamHandler<E> create(
            final Subscriber<? super EventBatch<E>> subscriber,
            final Function<? super String, ? extends EventBatch<E>> parser) {
        return new EventStreamHandler<>(subscriber, parser);
    }

    @Override
    public void onThrowable(final Throwable t) {
        abort(t);
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart bodyPart) {

        if (subscriber.isUnsubscribed()) {
            return State.ABORT;
        }

        // Sometimes, we get a body part containing nothing
        if (bodyPart.length() > 0) {
            final byte[] bytes = bodyPart.getBodyPartBytes();
            String payload = new String(bytes, charset);

            LOG.trace(payload);

            // If the payload is valid, it is a complete event, therefore, if the bodypart before was only part of
            // an event, it was corrupted and cannot be parsed.
            if (isValidJson(payload)) {
                bodyParts.clear();
                subscriber.onNext(parser.apply(payload));
            } else {
                bodyParts.add(bodyPart);
                payload = combinePayload(bodyParts, charset);

                if (isValidJson(payload)) {
                    bodyParts.clear();
                    subscriber.onNext(parser.apply(payload));
                } else {
                    LOG.debug(
                        "The response body part doesn't contain a full event. Wait for next body part to concatenate.");
                }
            }
        }

        return State.CONTINUE;
    }

    @Override
    public State onStatusReceived(final HttpResponseStatus responseStatus) {

        if (subscriber.isUnsubscribed()) {
            return State.ABORT;
        }

        if (responseStatus.getStatusCode() != StatusCodes.OK) {
            return abort(new IllegalStateException(
                        "Unexpected status code " + responseStatus.getStatusText() + " received."));
        }

        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(final HttpResponseHeaders headers) {

        if (subscriber.isUnsubscribed()) {
            return State.ABORT;
        }

        final HttpHeaders httpHeaders = headers.getHeaders();

        // Try to get the encoding
        final Charset parsedCharset;
        try {
            parsedCharset =
                Optional.ofNullable(httpHeaders.get(CONTENT_TYPE))    //
                        .map(MediaType::parse)                        //
                        .map(MediaType::charset)                      //
                        .map(com.google.common.base.Optional::orNull) //
                        .orElse(StandardCharsets.UTF_8);
        } catch (final IllegalArgumentException e) {
            return abort(e);
        }

        charset = parsedCharset;

        // Get the Nakadi session id
        if (!httpHeaders.contains(NAKADI_CLIENT_IDENTIFIER_HEADER)) {
            return abort(new IllegalStateException("Missing " + NAKADI_CLIENT_IDENTIFIER_HEADER + " HTTP header"));
        }

        sessionId.onNext(httpHeaders.get(NAKADI_CLIENT_IDENTIFIER_HEADER));
        sessionId.onCompleted();

        return State.CONTINUE;
    }

    public Single<String> getSessionId() {
        return sessionId.toSingle();
    }

    @Override
    public EventStreamHandler<E> onCompleted() {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onCompleted();
        }

        return this;
    }

    private static String combinePayload(final List<HttpResponseBodyPart> bodyParts, final Charset charset) {
        final int length = bodyParts.stream().map(HttpResponseBodyPart::length).reduce(0, (a, b) -> a + b);
        final ByteBuffer target = ByteBuffer.wrap(new byte[length]);

        bodyParts.stream().map(HttpResponseBodyPart::getBodyPartBytes).forEach(target::put);

        return new String(target.array(), charset);
    }

    /**
     * Checks if the bodypart contains only complete events (valid json).
     *
     * @param  data  The response data
     */
    private static boolean isValidJson(final String data) {
        try {
            new Gson().fromJson(data, Object.class);
        } catch (final JsonSyntaxException ignored) {
            return false;
        }

        return true;
    }

    private State abort(final Throwable error) {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onError(error);
        }

        return State.ABORT;
    }
}
