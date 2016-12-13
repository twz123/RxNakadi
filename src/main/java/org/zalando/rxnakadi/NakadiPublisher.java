package org.zalando.rxnakadi;

import static java.nio.charset.StandardCharsets.UTF_8;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Type;

import java.net.URI;

import java.util.List;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.domain.PublishingProblem;
import org.zalando.rxnakadi.hystrix.HystrixCommands;

import org.zalando.undertaking.oauth2.AccessToken;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;

import io.netty.handler.codec.http.HttpResponseStatus;

import rx.Completable;
import rx.Observable;
import rx.Single;

import rx.exceptions.Exceptions;

import rx.subjects.AsyncSubject;

/**
 * Write events into the Nakadi event bus.
 */
public final class NakadiPublisher {

    private static final Type PUBLISHING_PROBLEM_LIST = new TypeToken<List<PublishingProblem>>() {
            // capture generic type
        }.getType();

    private final AsyncHttpClient client;
    private final Single<AccessToken> accessToken;
    private final Gson gson;

    private static final HystrixObservableCommand.Setter SETTER =
        HystrixObservableCommand.Setter.withGroupKey(                            //
                HystrixCommandGroupKey.Factory.asKey("nakadi"))                  //
            .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(10000)
                    .withExecutionIsolationSemaphoreMaxConcurrentRequests(1000)) //
            .andCommandKey(HystrixCommandKey.Factory.asKey("publishEvents"));

    @Inject
    NakadiPublisher(@Internal final AsyncHttpClient client, final Single<AccessToken> accessToken, final Gson gson) {
        this.client = requireNonNull(client);
        this.accessToken = requireNonNull(accessToken);
        this.gson = requireNonNull(gson);
    }

    public <E extends NakadiEvent> Completable publishEvents(final URI nakadiUrl, final EventType eventType,
            final List<E> events) {
        requireNonNull(events);

        final byte[] payload;
        try {
            payload = gson.toJson(events).getBytes(UTF_8);
        } catch (final Throwable t) {
            Exceptions.throwIfFatal(t);
            return Completable.error(t);
        }

        final Single<Response> publishResult =                                           //
            accessToken.map(token -> buildRequest(nakadiUrl, eventType, token, payload)) //
                       .flatMap(requestBuilder ->
                               HystrixCommands.withRetries(() -> {
                                       return createHystrixObservable(requestBuilder, eventType);
                                   },
                                   3));

        final AsyncSubject<Response> subject = AsyncSubject.create();
        publishResult.subscribe(subject);
        return subject.toCompletable();
    }

    private HystrixObservableCommand<Response> createHystrixObservable(final BoundRequestBuilder requestBuilder,
            final EventType eventType) {
        return new HystrixObservableCommand<Response>(SETTER) {
            @Override
            protected Observable<Response> construct() {
                return AsyncHttpSingle.create(requestBuilder) //
                                      .flatMapObservable(response ->
                                              validateResponse(eventType, response));
            }
        };
    }

    private BoundRequestBuilder buildRequest(final URI nakadiUrl, final EventType eventType,
            final AccessToken accessToken, final byte[] payload) {
        return
            client.preparePost(String.format("%s/event-types/%s/events", nakadiUrl, eventType)) //
                  .setHeader(HttpHeaders.ACCEPT, "application/json")                            //
                  .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())         //
                  .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken.getValue())     //
                  .setBody(payload);
    }

    private Observable<Response> validateResponse(final EventType eventType, final Response response) {
        final int statusCode = response.getStatusCode();
        if (statusCode == HttpResponseStatus.OK.code()) {
            return Observable.just(response);
        }

        final RuntimeException error;

        if (statusCode == HttpResponseStatus.UNPROCESSABLE_ENTITY.code()) {
            error = new NakadiPublishingException(eventType,
                    gson.fromJson(response.getResponseBody(), PUBLISHING_PROBLEM_LIST));
        } else {
            error = new UnsupportedOperationException( //
                    String.format("Unsupported status code: %d: %s", statusCode, response.getResponseBody()));
        }

        if (statusCode < 400 || statusCode > 499) {
            return Observable.error(error);
        }

        return Observable.error(new HystrixBadRequestException(error.getMessage(), error));
    }
}
