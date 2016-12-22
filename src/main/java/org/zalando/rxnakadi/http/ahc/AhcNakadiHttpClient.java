package org.zalando.rxnakadi.http.ahc;

import static java.util.Objects.requireNonNull;

import static org.asynchttpclient.util.HttpConstants.Methods.POST;

import static org.zalando.rxnakadi.http.NakadiHttp.EVENTS_DELIMITER_PATTERN;
import static org.zalando.rxnakadi.http.NakadiHttp.HYSTRIX_GROUP;
import static org.zalando.rxnakadi.http.NakadiHttp.JSON_TYPE;
import static org.zalando.rxnakadi.http.NakadiHttp.PUBLISHING_PROBLEM_LIST;
import static org.zalando.rxnakadi.http.NakadiHttp.X_NAKADI_STREAM_ID;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.contentType;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.onClientError;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.responseString;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.statusCode;
import static org.zalando.rxnakadi.http.ahc.DelimitedJsonStreamTarget.JSON_STREAM_TYPE;
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
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.zalando.rxnakadi.NakadiPublishingException;
import org.zalando.rxnakadi.SubscriptionDescriptor;
import org.zalando.rxnakadi.domain.NakadiSubscription;
import org.zalando.rxnakadi.http.NakadiHttp;
import org.zalando.rxnakadi.http.NakadiHttpClient;
import org.zalando.rxnakadi.hystrix.HystrixCommands;
import org.zalando.rxnakadi.inject.Nakadi;

import org.zalando.undertaking.oauth2.AccessToken;

import com.google.common.collect.ImmutableMap;

import com.google.gson.Gson;

import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;

import rx.Completable;
import rx.Observable;
import rx.Single;

import rx.subscriptions.Subscriptions;

public final class AhcNakadiHttpClient implements NakadiHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(AhcNakadiHttpClient.class);

    private final Uri nakadiEndpoint;
    private final AsyncHttpClient http;
    private final Single<AccessToken> accessToken;
    private final Gson gson;

    @Inject
    AhcNakadiHttpClient(@Nakadi final URI nakadiEndpoint, final AsyncHttpClient http,
            @Nakadi final Single<AccessToken> accessToken, final Gson gson) {
        this.nakadiEndpoint = Uri.create(nakadiEndpoint.toString());
        this.http = requireNonNull(http);
        this.accessToken = requireNonNull(accessToken);
        this.gson = requireNonNull(gson);
    }

    @Override
    public Single<NakadiSubscription> getSubscription( //
            final EventType eventType, final SubscriptionDescriptor sd) {
        requireNonNull(eventType);
        requireNonNull(sd);

        final NakadiSubscription subscription = new NakadiSubscription( //
                sd.getOwningApplication(),                              //
                Collections.singleton(eventType),                       //
                sd.getConsumerGroup());

        final Request request =
            new RequestBuilder(POST).setUri(buildUri("subscriptions"))              //
                                    .setHeader(ACCEPT, JSON_TYPE.toString())        //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString()) //
                                    .setBody(gson.toJson(subscription))             //
                                    .build();

        return http(request).<NakadiSubscription>flatMap(dispatch(statusCode(),              //
                                    onAnyOf(200, 201).dispatch(contentType(),                //
                                        on(JSON_TYPE).map(parse(NakadiSubscription.class))), //
                                    onClientError().error(this::hystrixBadRequest)))         //
                            .compose(hystrix("getSubscription"));
    }

    @Override
    public Observable<String> getEventsForType(final EventType eventType) {
        return getEvents(buildUri("event-types/%s/events", eventType.toString()), null);
    }

    @Override
    public Observable<String> getEventsForSubscription(final String subscriptionId, final Consumer<String> streamId) {
        checkArgument(!subscriptionId.isEmpty(), "subscriptionId may not be empty");
        requireNonNull(streamId);
        return getEvents(buildUri("subscriptions/%s/events", subscriptionId), streamId);
    }

    @Override
    public Completable commitCursor(final Optional<Object> cursor, final String subscriptionId, final String streamId) {

        final Request request =
            new RequestBuilder(POST).setUri(buildUri("subscriptions/%s/cursors", subscriptionId)) //
                                    .setHeader(X_NAKADI_STREAM_ID, streamId)                      //
                                    .setHeader(ACCEPT, JSON_TYPE.toString())                      //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())               //
                                    .setBody(gson.toJson(
                                            ImmutableMap.of("items",
                                                cursor.map(Collections::singleton)                //
                                                .orElse(Collections.emptySet()))))                //
                                    .build();

        return http(request).flatMap(dispatch(statusCode(),                                    //
                                    on(200).dispatch(contentType(), on(JSON_TYPE).pass()),     //
                                    on(204).pass(),                                            //
                                    onClientError().error(this::hystrixBadRequest)))           //
                            .compose(hystrix("commitCursor"))                                  //
                            .toCompletable();
    }

    @Override
    public Completable publishEvents(final EventType eventType, final List<?> events) {
        requireNonNull(eventType);

        if (events.isEmpty()) {
            return Completable.complete();
        }

        final Request request =
            new RequestBuilder(POST).setUri(buildUri("event-types/%s/events", eventType.toString())) //
                                    .setHeader(ACCEPT, JSON_TYPE.toString())                         //
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())                  //
                                    .setBody(gson.toJson(events))                                    //
                                    .build();

        return http(request).flatMap(dispatch(statusCode(),                                    //
                                    on(200).dispatch(contentType(), on(JSON_TYPE).pass()),     //
                                    on(422).dispatch(contentType(), on(JSON_TYPE).error(response ->
                                                publishingProblem(eventType, response))),      //
                                    onClientError().error(this::hystrixBadRequest)))           //
                            .compose(hystrix("publishEvents"))                                 //
                            .toCompletable();
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
                                          LOG.debug("Request: [{} {}], headers: [{}], body: [{}]", request.getMethod(),
                                              request.getUri(), request.getHeaders().entries(),
                                              request.getStringData()));
                          })                                                                            //
                          .doOnSuccess(response ->
                                  LOG.debug("Response: [{} {}], headers: [{}], body: [{}]", response.getStatusCode(),
                                      response.getStatusText(), response.getHeaders().entries(),
                                      response.getResponseBody()));
    }

    private static <T> Single.Transformer<T, T> hystrix(final String commandKeyName) {
        final HystrixObservableCommand.Setter setter =                  //
            HystrixObservableCommand.Setter.withGroupKey(HYSTRIX_GROUP) //
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
                        LOG.debug("Request: [{} {}]", request.getMethod(), request.getUri());

                        final AsyncHandler<?> handler = StreamedAsyncAdapter.withTarget( //
                                new DelimitedJsonStreamTarget(subscriber, EVENTS_DELIMITER_PATTERN, nakadiStreamId));
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

    private HystrixBadRequestException publishingProblem(final EventType eventType, final Response response) {
        final NakadiPublishingException publishingException = new NakadiPublishingException(eventType,
                response.getHeader(NakadiHttp.X_FLOW_ID),
                gson.fromJson(response.getResponseBody(), PUBLISHING_PROBLEM_LIST));
        return new HystrixBadRequestException(publishingException.getMessage(), publishingException);
    }

    private HystrixBadRequestException hystrixBadRequest(final Response response) {
        return new HystrixBadRequestException(responseString(response));
    }
}
