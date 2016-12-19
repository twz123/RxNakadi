package org.zalando.rxnakadi.http;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

import static java.util.Objects.requireNonNull;

import static org.zalando.rxnakadi.http.AhcResponseDispatch.responseString;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.statusCode;
import static org.zalando.rxnakadi.http.NakadiHttpClient.X_NAKADI_STREAM_ID;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.dispatch;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.on;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

import java.nio.charset.Charset;

import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;

import org.reactivestreams.Publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.Transformers;

import com.google.common.net.MediaType;

import io.netty.handler.codec.http.HttpHeaders;

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

        final int statusCode = responseStatus.getStatusCode();
        LOG.debug("Response: [{} {}], headers: [{}]", statusCode, responseStatus.getStatusText(),
            responseHeaders.entries());

        if (subscriber.isUnsubscribed()) {
            return false;
        }

        if (statusCode == 200) {
            ok(responseHeaders, publisher);
            return true;
        }

        RxReactiveStreams.toObservable(publisher)                                                //
                         .lift(StreamedResponses.takeBytes(4096))                                //
                         .compose(StreamedResponses.toResponse(responseStatus,
                                 new HttpResponseHeaders(responseHeaders)))                      //
                         .toSingle()                                                             //
                         .flatMap(dispatch(statusCode(),                                         //
                                 on(409).error(response ->
                                         new IllegalStateException(responseString(response)))))  //
                         .toCompletable()                                                        //
                         .subscribe(subscriber);
        return true;
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

        final Charset charset = contentType.charset().or(ISO_8859_1);
        RxReactiveStreams.toObservable(publisher)                          // reactive streams -> RxJava 1.x
                         .map(HttpResponseBodyPart::getBodyPartBytes)      // parts -> bytes
                         .compose(bytes -> Strings.decode(bytes, charset)) // bytes -> chars
                         .compose(Transformers.split(delimiterPattern))    // chars -> chunks
                         .subscribe(subscriber);
    }
}
