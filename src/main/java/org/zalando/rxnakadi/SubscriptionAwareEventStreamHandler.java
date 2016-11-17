package org.zalando.rxnakadi;

import java.util.function.Function;

import org.asynchttpclient.HttpResponseHeaders;

import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.NakadiEvent;

import io.netty.handler.codec.http.HttpHeaders;

import rx.Single;
import rx.Subscriber;

import rx.subjects.AsyncSubject;

/**
 * Creates the desired Nakadi events by consuming a continuous stream of raw data.
 *
 * <p>This implementation can only be used when interacting with Nakadi's High Level API as it expects to receive the
 * {@code X-Nakadi-StreamId} HTTP header so that cursors can be committed for this particular stream.</p>
 */
public class SubscriptionAwareEventStreamHandler<E extends NakadiEvent> extends EventStreamHandler<E> {

    /**
     * The HTTP header specifying the identifier to use when committing the cursor.
     */
    public static final String NAKADI_CLIENT_IDENTIFIER_HEADER = "X-Nakadi-StreamId";

    /**
     * Identifier of the Nakadi session used for this processor.
     */
    private final AsyncSubject<String> clientId = AsyncSubject.create();

    protected SubscriptionAwareEventStreamHandler(final Subscriber<? super EventBatch<E>> subscriber,
            final Function<? super String, ? extends EventBatch<E>> parser) {
        super(subscriber, parser);
    }

    /**
     * Creates a new handler that publishes events of type {@link E} out of raw data.
     *
     * <p>Each valid event is passed to the subscriber for consuming.</p>
     *
     * @param  subscriber  target for the produced events
     * @param  parser      parsing function that converts JSON payload to events
     */
    public static <E extends NakadiEvent> SubscriptionAwareEventStreamHandler<E> create(
            final Subscriber<? super EventBatch<E>> subscriber,
            final Function<? super String, ? extends EventBatch<E>> parser) {
        return new SubscriptionAwareEventStreamHandler<>(subscriber, parser);
    }

    @Override
    public State onHeadersReceived(final HttpResponseHeaders headers) {

        if (subscriber.isUnsubscribed()) {
            return State.ABORT;
        }

        final HttpHeaders httpHeaders = headers.getHeaders();

        // Try to get the encoding
        try {
            parseCharset(headers.getHeaders());
        } catch (final RuntimeException e) {
            return abort(e);
        }

        // Get the Nakadi session id
        if (!httpHeaders.contains(NAKADI_CLIENT_IDENTIFIER_HEADER)) {
            return abort(new IllegalStateException("Missing " + NAKADI_CLIENT_IDENTIFIER_HEADER + " HTTP header"));
        }

        clientId.onNext(httpHeaders.get(NAKADI_CLIENT_IDENTIFIER_HEADER));
        clientId.onCompleted();

        return State.CONTINUE;
    }

    public Single<String> getClientId() {
        return clientId.toSingle();
    }
}
