package org.zalando.rxnakadi.http;

import static java.util.Objects.requireNonNull;

import static org.zalando.rxnakadi.http.AhcResponseDispatch.contentType;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.onClientError;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.responseString;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.statusCode;
import static org.zalando.rxnakadi.http.DelimitedJsonStreamTarget.JSON_STREAM_TYPE;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.dispatch;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.on;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;

import java.net.URI;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.asynchttpclient.uri.Uri;

import org.zalando.rxnakadi.EventType;
import org.zalando.rxnakadi.Nakadi;
import org.zalando.rxnakadi.NakadiSubscription;
import org.zalando.rxnakadi.hystrix.HystrixCommands;

import org.zalando.undertaking.oauth2.AccessToken;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;

import com.google.gson.Gson;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;

import io.netty.handler.codec.http.HttpMethod;

import rx.Observable;
import rx.Single;

import rx.subscriptions.Subscriptions;

public class NakadiHttpClient {

    /**
     * The HTTP header specifying the identifier to use when committing the cursor.
     */
    static final String X_NAKADI_STREAM_ID = "X-Nakadi-StreamId";

    private final MediaType jsonType = MediaType.create("application", "json");
    private final MediaType starJsonType = MediaType.create("application", "*+json");

    /**
     * Used to split the character stream into individual JSON chunks of Nakadi's flavor of the
     * {@literal "application/x-json-stream"} Media Type. According to the <a
     * href="https://github.com/zalando/nakadi/blob/R2016_12_08_RC1/api/nakadi-event-bus-api.yaml#L460">Nakadi API
     * specification</a>, this is always the newline character.
     */
    private final Pattern eventsDelimiterPattern = Pattern.compile("\\n", Pattern.LITERAL);

    private final HystrixCommandGroupKey groupKey = HystrixCommandGroupKey.Factory.asKey("nakadi");

    private final Uri nakadiEndpoint;
    private final AsyncHttpClient http;
    private final Single<AccessToken> accessToken;
    private final Gson gson;

    @Inject
    public NakadiHttpClient(@Nakadi final URI nakadiEndpoint, final AsyncHttpClient http,
            final Single<AccessToken> accessToken, final Gson gson) {
        this.nakadiEndpoint = Uri.create(nakadiEndpoint.toString());
        this.http = requireNonNull(http);
        this.accessToken = requireNonNull(accessToken);
        this.gson = requireNonNull(gson);
    }

    public Single<NakadiSubscription> getSubscription( //
            final EventType eventType, final String owningApplication, final String consumerGroup) {

        final Uri uri = buildUri("subscriptions");
        final String payload = gson.toJson(new NakadiSubscription( //
                    owningApplication, Collections.singleton(eventType), consumerGroup));

        return request(() ->
                                    requestFor(POST, uri)                               //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())     //
                                    .setBody(payload))                                  //
            .<NakadiSubscription>flatMap(dispatch(statusCode(),                         //
                                    on(201).dispatch(contentType(),                     //
                                        on(jsonType).map(parse(NakadiSubscription.class))), //
                                    onClientError().error(this::badResponse)))          //
                            .compose(hystrix("getSubscription"));
    }

    public Observable<String> getEventsForType(final EventType eventType) {
        return getEvents(buildUri("event-types/%s/events", eventType.toString()), null);
    }

    public Observable<String> getEventsForSubscription(final String subscriptionId, final Consumer<String> streamId) {
        checkArgument(subscriptionId.isEmpty(), "subscriptionId may not be empty");
        requireNonNull(streamId);
        return getEvents(buildUri("subscriptions/%s/events", subscriptionId), streamId);
    }

    public Single<String> commitCursor(final Optional<Object> cursor, final String subscriptionId,
            final String streamId) {

        final Uri uri = buildUri("subscriptions/%s/cursors", subscriptionId);
        final String payload =                                   //
            cursor.map(Collections::singleton)                   //
                  .map(items -> ImmutableMap.of("items", items)) //
                  .map(gson::toJson)                             //
                  .orElse("[]");

        return request(() ->
                                    requestFor(HttpMethod.POST, uri)                        //
                                    .setHeader(X_NAKADI_STREAM_ID, streamId)                //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())         //
                                    .setBody(payload))                                      //
            .<CursorCommitResult>flatMap(dispatch(statusCode(),                             //
                                    on(200).dispatch(contentType(),                         //
                                        on(jsonType).map(parse(CursorCommitResult.class))), //
                                    onClientError().error(this::badResponse)))              //
                            .compose(hystrix("commitCursor"))                               //
                            .map(result -> result.result);
    }

    private Single<Response> request(final Supplier<RequestBuilder> requestSupplier) {
        return accessToken.map(token -> {
                              final RequestBuilder builder = requestSupplier.get();
                              builder.setHeader(AUTHORIZATION, token.getTypeAndValue());
                              builder.setHeader(ACCEPT, jsonType.toString());
                              builder.addHeader(ACCEPT, starJsonType.toString());
                              return builder.build();
                          }) //
                          .flatMap(requestWithAuth -> {
                              return AsyncHttpSingle.create(handler ->
                                          http.executeRequest(requestWithAuth, handler));
                          });
    }

    private <T> T parse(final Response response, final Function<? super String, ? extends T> responseParser) {
        final int statusCode = response.getStatusCode();

        if (statusCode == 200) {
            final String contentTypeString = response.getContentType();
            if (contentTypeString == null) {
                throw new UnsupportedOperationException("No content type: " + responseString(response));
            }

            final MediaType contentType;
            try {
                contentType = MediaType.parse(contentTypeString);
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Illegal content type " + contentTypeString + ": "
                        + responseString(response), e);
            }

            if (contentType.is(jsonType) || contentType.is(starJsonType)) {
                return responseParser.apply(response.getResponseBody());
            }

            throw new UnsupportedOperationException( //
                "Unsupported content type " + contentType + ": " + responseString(response));
        }

        if (statusCode >= 400 && statusCode <= 499) {
            throw new HystrixBadRequestException(responseString(response));
        }

        throw new UnsupportedOperationException(responseString(response));
    }

    private <T> Single.Transformer<T, T> hystrix(final String commandKeyName) {
        final HystrixObservableCommand.Setter setter =             //
            HystrixObservableCommand.Setter.withGroupKey(groupKey) //
                                           .andCommandKey(HystrixCommandKey.Factory.asKey(commandKeyName));
        return single -> {
            return HystrixCommands.withRetries(() -> toHystrixCommand(setter, single.toObservable()), 3);
        };
    }

    private static <T> HystrixObservableCommand<T> toHystrixCommand(final HystrixObservableCommand.Setter setter,
            final Observable<T> observable) {
        return new HystrixObservableCommand<T>(setter) {
            @Override
            protected Observable<T> construct() {
                return observable;
            }
        };
    }

    private Observable<String> getEvents(final Uri uri, final Consumer<String> nakadiStreamId) {
        return accessToken.flatMapObservable(token -> {
                final Request request =
                    requestFor(GET, uri)                           //
                    .setHeader(AUTHORIZATION, token.getTypeAndValue()) //
                    .setHeader(ACCEPT, JSON_STREAM_TYPE.toString()) //
                    .build();

                return Observable.create(subscriber -> {
                        final AsyncHandler<?> handler = StreamedAsyncAdapter.withTarget( //
                                new DelimitedJsonStreamTarget(subscriber, eventsDelimiterPattern, nakadiStreamId));
                        subscriber.add(Subscriptions.from(http.executeRequest(request, handler)));
                    });
            });
    }

    private Uri buildUri(final String template, final String... pathParams) {
        final Object[] escapedPathParams = Stream.of(pathParams).map(urlPathSegmentEscaper()::escape).toArray();
        return Uri.create(nakadiEndpoint, String.format(template, escapedPathParams));
    }

    private static RequestBuilder requestFor(final HttpMethod method, final Uri uri) {
        return new RequestBuilder(method.name()).setUri(uri);
    }

    private <T> Function<Response, T> parse(final Class<T> clazz) {
        requireNonNull(clazz);
        return response -> gson.fromJson(response.getResponseBody(), clazz);
    }

    private HystrixBadRequestException badResponse(final Response response) {
        return new HystrixBadRequestException(responseString(response));
    }

    private static final class CursorCommitResult {
        String result;
    }
}
