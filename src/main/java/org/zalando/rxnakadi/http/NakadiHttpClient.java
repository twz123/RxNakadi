package org.zalando.rxnakadi.http;

import static java.util.Objects.requireNonNull;

import static org.asynchttpclient.util.HttpConstants.Methods.POST;

import static org.zalando.rxnakadi.http.AhcResponseDispatch.contentType;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.onClientError;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.responseString;
import static org.zalando.rxnakadi.http.AhcResponseDispatch.statusCode;
import static org.zalando.rxnakadi.http.DelimitedJsonStreamTarget.JSON_STREAM_TYPE;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.dispatch;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.on;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.onAnyOf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;

import java.net.URI;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import rx.Observable;
import rx.Single;

import rx.subscriptions.Subscriptions;

public class NakadiHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiHttpClient.class);

    /**
     * The HTTP header specifying the identifier to use when committing the cursor.
     */
    static final String X_NAKADI_STREAM_ID = "X-Nakadi-StreamId";

    private final MediaType jsonType = MediaType.JSON_UTF_8.withoutParameters();

    /**
     * Used to split the character stream into individual JSON chunks of Nakadi's flavor of the
     * {@literal "application/x-json-stream"} Media Type. According to the <a
     * href="https://github.com/zalando/nakadi/blob/R2016_12_08_RC1/api/nakadi-event-bus-api.yaml#L460">Nakadi API
     * specification</a>, this is always the newline character.
     */
    private final Pattern eventsDelimiterPattern = Pattern.compile("\n", Pattern.LITERAL);

    private final HystrixCommandGroupKey groupKey = HystrixCommandGroupKey.Factory.asKey("nakadi");

    private final Uri nakadiEndpoint;
    private final AsyncHttpClient http;
    private final Single<AccessToken> accessToken;
    private final Gson gson;

    @Inject
    public NakadiHttpClient(@Nakadi final URI nakadiEndpoint, final AsyncHttpClient http,
            @Nakadi final Single<AccessToken> accessToken, final Gson gson) {
        this.nakadiEndpoint = Uri.create(nakadiEndpoint.toString());
        this.http = requireNonNull(http);
        this.accessToken = requireNonNull(accessToken);
        this.gson = requireNonNull(gson);
    }

    public Single<NakadiSubscription> getSubscription( //
            final EventType eventType, final String owningApplication, final String consumerGroup) {

        final Request request =
            new RequestBuilder(POST).setUri(buildUri("subscriptions"))                        //
                                    .setHeader(ACCEPT, jsonType.toString())                   //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())           //
                                    .setBody(gson.toJson(
                                            new NakadiSubscription(                           //
                                                owningApplication, Collections.singleton(eventType), consumerGroup))) //
                                    .build();

        return http(request).<NakadiSubscription>flatMap(dispatch(statusCode(),             //
                                    onAnyOf(200, 201).dispatch(contentType(),               //
                                        on(jsonType).map(parse(NakadiSubscription.class))), //
                                    onClientError().error(this::hystrixBadRequest)))        //
                            .compose(hystrix("getSubscription"));
    }

    public Observable<String> getEventsForType(final EventType eventType) {
        return getEvents(buildUri("event-types/%s/events", eventType.toString()), null);
    }

    public Observable<String> getEventsForSubscription(final String subscriptionId, final Consumer<String> streamId) {
        checkArgument(!subscriptionId.isEmpty(), "subscriptionId may not be empty");
        requireNonNull(streamId);
        return getEvents(buildUri("subscriptions/%s/events", subscriptionId), streamId);
    }

    public Single<String> commitCursor(final Optional<Object> cursor, final String subscriptionId,
            final String streamId) {

        final Request request =
            new RequestBuilder(POST).setUri(buildUri("subscriptions/%s/cursors", subscriptionId)) //
                                    .setHeader(X_NAKADI_STREAM_ID, streamId)                      //
                                    .setHeader(ACCEPT, jsonType.toString())                       //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())               //
                                    .setBody(gson.toJson(
                                            ImmutableMap.of("items",
                                                cursor.map(Collections::singleton)                //
                                                .orElse(Collections.emptySet()))))                //
                                    .build();

        return http(request).<CursorCommitResult>flatMap(dispatch(statusCode(),                 //
                                    on(200).dispatch(contentType(),                             //
                                        on(jsonType).map(parse(CursorCommitResult.class))),     //
                                    on(204).map(response -> new CursorCommitResult()),          //
                                    onClientError().error(this::hystrixBadRequest)))            //
                            .compose(hystrix("commitCursor"))                                   //
                            .map(result -> result.result);
    }

    private Single<Response> http(final Request request) {
        return accessToken.map(token -> {
                              final RequestBuilder builder = new RequestBuilder(request);
                              builder.setHeader(AUTHORIZATION, token.getTypeAndValue());
                              return builder.build();
                          })                                                                            //
                          .flatMap(requestWithAuth -> {
                              return AsyncHttpSingle.create(handler ->
                                          http.executeRequest(requestWithAuth, handler))                //
                                  .doOnSubscribe(() ->
                                          LOG.info("Request: [{} {}], headers: [{}], body: [{}]", request.getMethod(),
                                              request.getUri(), request.getHeaders().entries(),
                                              request.getStringData()));
                          })                                                                            //
                          .doOnSuccess(response ->
                                  LOG.info("Response: [{} {}], headers: [{}], body: [{}]", response.getStatusCode(),
                                      response.getStatusText(), response.getHeaders().entries(),
                                      response.getResponseBody()));
    }

    private <T> Single.Transformer<T, T> hystrix(final String commandKeyName) {
        final HystrixObservableCommand.Setter setter =             //
            HystrixObservableCommand.Setter.withGroupKey(groupKey) //
                                           .andCommandKey(HystrixCommandKey.Factory.asKey(commandKeyName));
        return single -> {
            return HystrixCommands.withRetries(() -> toHystrixCommand(setter, single.toObservable()), 3);
        };
    }

    private static <T> HystrixObservableCommand<T> toHystrixCommand( //
            final HystrixObservableCommand.Setter setter, final Observable<T> observable) {
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
                    new RequestBuilder().setUri(uri)               //
                    .setRequestTimeout(-1)                         //
                    .setHeader(AUTHORIZATION, token.getTypeAndValue()) //
                    .setHeader(ACCEPT, JSON_STREAM_TYPE.toString()) //
                    .build();

                return Observable.create(subscriber -> {
                        LOG.info("Request: [{} {}]", request.getMethod(), request.getUri());

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

    private <T> Function<Response, T> parse(final Class<T> clazz) {
        requireNonNull(clazz);
        return response -> gson.fromJson(response.getResponseBody(), clazz);
    }

    private HystrixBadRequestException hystrixBadRequest(final Response response) {
        return new HystrixBadRequestException(responseString(response));
    }

    private static final class CursorCommitResult {
        String result;
    }
}
