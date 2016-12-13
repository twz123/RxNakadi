package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.Optional;
import java.util.function.Function;

import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;

import org.asynchttpclient.handler.StreamedAsyncHandler;

import org.reactivestreams.Publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.NakadiEvent;

import com.google.common.net.MediaType;

import io.netty.handler.codec.http.HttpHeaders;

import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;

import rx.observables.StringObservable;

/**
 * Creates the desired Nakadi events by consuming a continuous stream of raw data.
 *
 * @param  <E>  Nakadi event type being produced
 */
class EventStreamHandler<E extends NakadiEvent> implements StreamedAsyncHandler<EventStreamHandler<E>> {

    /**
     * Logging instance of this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventStreamHandler.class);

    /**
     * Target, which receives the produced events.
     */
    protected final Subscriber<? super EventBatch<E>> subscriber;

    /**
     * Parses payloads.
     */
    private final Function<? super String, ? extends EventBatch<E>> parser;

    /**
     * Charset being used to decode the endpoints payload.
     */
    protected volatile Charset charset;

    protected EventStreamHandler(final Subscriber<? super EventBatch<E>> subscriber,
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
    public State onStatusReceived(final HttpResponseStatus responseStatus) {

        if (subscriber.isUnsubscribed()) {
            return State.ABORT;
        }

        if (responseStatus.getStatusCode() != OK.code()) {
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

        if (headers.isTrailling()) {
            return State.CONTINUE;
        }

        try {
            parseCharset(headers.getHeaders());
        } catch (final RuntimeException e) {
            return abort(e);
        }

        return State.CONTINUE;
    }

    @Override
    public void onThrowable(final Throwable t) {
        abort(t);
    }

    @Override
    public State onStream(final Publisher<HttpResponseBodyPart> publisher) {

        // parts -> bytes -> strings -> lines -> events
        final Observable<HttpResponseBodyPart> parts = RxReactiveStreams.toObservable(publisher);
        final Observable<byte[]> bytes = parts.map(HttpResponseBodyPart::getBodyPartBytes);
        final Observable<String> strings = StringObservable.decode(bytes, charset);
        final Observable<String> lines = StringObservable.byLine(strings);
        final Observable<? extends EventBatch<E>> events = lines.map(parser::apply);

        events.subscribe(subscriber);

        return State.CONTINUE;
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart bodyPart) {
        LOG.warn("Body part received, but streaming is used.");
        return State.CONTINUE;
    }

    @Override
    public EventStreamHandler<E> onCompleted() {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onCompleted();
        }

        return this;
    }

    protected void parseCharset(final HttpHeaders httpHeaders) {

        // Try to get the encoding
        charset =
            Optional.ofNullable(httpHeaders.get(CONTENT_TYPE))    //
                    .map(MediaType::parse)                        //
                    .map(MediaType::charset)                      //
                    .map(com.google.common.base.Optional::orNull) //
                    .orElse(StandardCharsets.UTF_8);
    }

    protected State abort(final Throwable error) {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onError(error);
        }

        return State.ABORT;
    }
}
