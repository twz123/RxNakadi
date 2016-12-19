package org.zalando.rxnakadi.http;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

import static java.util.Objects.requireNonNull;

import static org.zalando.rxnakadi.http.NakadiHttpClient.X_NAKADI_STREAM_ID;

import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.nio.charset.Charset;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;

import org.reactivestreams.Publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.Transformers;

import com.google.common.net.MediaType;

import io.netty.handler.codec.http.HttpHeaders;

import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;

/**
 * A {@link StreamedAsyncAdapter.Target Streamed Target} that processes HTTP responses of type
 * {@literal "application/x-json-stream"} that are delimited by some character sequence, typically some JSON whitespace
 * characters that that may not be used unencoded inside JSON strings.
 */
final class DelimitedJsonStreamTarget extends SubscriptionTarget<String> {

    private static final Logger LOG = LoggerFactory.getLogger(DelimitedJsonStreamTarget.class);

    static final MediaType JSON_STREAM_TYPE = MediaType.create("application", "x-json-stream");

    private final Pattern delimiterPattern;

    private final Consumer<String> nakadiStreamId;

    public DelimitedJsonStreamTarget(final Subscriber<? super String> subscriber, final Pattern delimiterPattern,
            final Consumer<String> nakadiStreamId) {
        super(subscriber);
        this.delimiterPattern = requireNonNull(delimiterPattern);
        this.nakadiStreamId = nakadiStreamId;
    }

    @Override
    public boolean startBodyStream(final HttpResponseStatus responseStatus, final HttpHeaders responseHeaders,
            final Publisher<HttpResponseBodyPart> publisher) {

        LOG.info("Response: [{} {}], headers: [{}]", responseStatus.getStatusCode(), responseStatus.getStatusText(),
            responseHeaders.entries());

        if (subscriber.isUnsubscribed()) {
            return false;
        }

        if (responseStatus.getStatusCode() == 200) {
            ok(responseHeaders, publisher);
            return true;
        }

        if (responseStatus.getStatusCode() == 409) {
            return conflict(responseStatus, responseHeaders, publisher);
        }

        throw new UnsupportedOperationException("Unsupported status code: " + responseStatus.getStatusCode());
    }

    private void ok(final HttpHeaders responseHeaders, final Publisher<HttpResponseBodyPart> publisher) {

        final String contentTypeString = responseHeaders.get(CONTENT_TYPE);
        if (contentTypeString == null) {
            throw new UnsupportedOperationException("No content type");
        }

        final MediaType contentType;
        try {
            contentType = MediaType.parse(contentTypeString);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal content type " + contentTypeString, e);
        }

        if (!contentType.is(JSON_STREAM_TYPE)) {
            throw new UnsupportedOperationException("Unsupported content type " + contentTypeString);
        }

        if (nakadiStreamId != null) {
            nakadiStreamId.accept(responseHeaders.get(X_NAKADI_STREAM_ID));
        }

        final Charset charset = charset(contentType);
        RxReactiveStreams.toObservable(publisher)                          // reactive streams -> RxJava 1.x
                         .map(HttpResponseBodyPart::getBodyPartBytes)      // parts -> bytes
                         .compose(bytes -> Strings.decode(bytes, charset)) // bytes -> chars
                         .compose(Transformers.split(delimiterPattern))    // chars -> chunks
                         .subscribe(subscriber);
    }

    private boolean conflict(final HttpResponseStatus responseStatus, final HttpHeaders responseHeaders,
            final Publisher<HttpResponseBodyPart> publisher) {

        Observable
            .just(responseHeaders.get(CONTENT_TYPE))                                                                   //
            .filter(Objects::nonNull)                                                                                  //
            .map(MediaType::parse)                                                                                     //
            .filter(contentType -> contentType.is(MediaType.create("application", "problem+json")))                    //
            .zipWith(contentLength(responseHeaders),
                (contentType, contentLength) -> toResponseString(charset(contentType), contentLength, publisher))      //
            .flatMap(observable -> observable)                                                                         //
            .onErrorResumeNext(error ->
                    Observable.error(new IllegalStateException("Conflict: " + responseStatus.getUri(), error)))        //
            .flatMap(response ->
                    Observable.error(
                        new IllegalStateException("Conflict: " + responseStatus.getUri() + ": " + response)))          //
            .switchIfEmpty(Observable.error(new IllegalStateException("Conflict: " + responseStatus.getUri())))
            .toCompletable()                                                                                           //
            .subscribe(subscriber);

        return !subscriber.isUnsubscribed();
    }

    private static Observable<Long> contentLength(final HttpHeaders responseHeaders) {
        return Observable.just(responseHeaders.get(CONTENT_LENGTH)) //
                         .map(contentLength -> contentLength == null ? -1 : Long.valueOf(contentLength));
    }

    private static Observable<String> toResponseString(final Charset charset, final long contentLength,
            final Publisher<HttpResponseBodyPart> publisher) {

        if (contentLength > 32 * 1024) {
            return Observable.error(new UnsupportedOperationException("Response body too large: " + contentLength));
        }

        return RxReactiveStreams.toObservable(publisher) //
                                .collect(() ->
                                        new ByteArrayOutputStream(contentLength < 0 ? 512 : (int) contentLength),
                                    (bytes, part) -> {
                                        try {
                                            bytes.write(part.getBodyPartBytes());
                                        } catch (final IOException e) {
                                            throw new RuntimeException(e.getMessage(), e);
                                        }
                                    })                   //
                                .map(bytes -> new String(bytes.toByteArray(), charset));
    }

    private static Charset charset(final MediaType contentType) {
        return contentType.charset().or(ISO_8859_1);
    }
}
