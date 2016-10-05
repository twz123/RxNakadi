package org.zalando.nakadilib;

import static java.nio.charset.StandardCharsets.UTF_8;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.lang.reflect.Type;

import java.net.URI;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.zalando.nakadilib.domain.NakadiEvent;
import org.zalando.nakadilib.domain.PublishingProblem;
import org.zalando.undertaking.oauth2.AccessToken;
import org.zalando.undertaking.utils.FixedAttemptsStrategy;

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

import io.undertow.util.StatusCodes;

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

        final Observable<Void> publishResult =                                                                  //
            accessToken.flatMapObservable(token ->
                               toResponseObservable(nakadiUrl, eventType, token, payload))                      //
                       .retry((attempt, error) ->
                               new FixedAttemptsStrategy(3).shouldBeRetried(attempt, error))                    //
                       .onErrorResumeNext(error -> Observable.error(unwrapError(error)));

        final AsyncSubject<Void> subject = AsyncSubject.create();
        publishResult.subscribe(subject);
        return subject.toCompletable();
    }

    private Observable<Void> toResponseObservable(final URI nakadiUrl, final EventType eventType,
            final AccessToken token, final byte[] payload) {
        final BoundRequestBuilder requestBuilder = buildRequest(nakadiUrl, eventType, token, payload);
        return Observable.defer(() ->
                    new HystrixObservableCommand<Void>(SETTER) {
                        @Override
                        protected Observable<Void> construct() {
                            return AsyncHttpSingle.create(requestBuilder) //
                                                  .flatMapObservable(response ->
                                                          validateResponse(eventType, response));
                        }
                    }.toObservable());
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

    private <T> Observable<T> validateResponse(final EventType eventType, final Response response) {
        final int statusCode = response.getStatusCode();
        if (statusCode == StatusCodes.OK) {
            return Observable.empty();
        }

        final RuntimeException error;

        if (statusCode == StatusCodes.UNPROCESSABLE_ENTITY) {
            error = new NakadiPublishingException(eventType,
                    gson.fromJson(response.getResponseBody(), PUBLISHING_PROBLEM_LIST));
        } else {
            error = new UnsupportedOperationException( //
                    "Unsupported status code: " + statusCode + ": " + response.getResponseBody());
        }

        if (statusCode < 400 || statusCode > 499) {
            return Observable.error(error);
        }

        return Observable.error(new HystrixBadRequestException(error.getMessage(), error));
    }

    private static Throwable unwrapError(final Throwable error) {

        if (error instanceof HystrixBadRequestException) {
            return firstNonNull(error.getCause(), error);
        } else if (error instanceof HystrixRuntimeException) {
            final Throwable cause = firstNonNull(error.getCause(), error);
            final HystrixRuntimeException.FailureType failureType = ((HystrixRuntimeException) error).getFailureType();

            switch (failureType) {

                case REJECTED_THREAD_EXECUTION :
                case REJECTED_SEMAPHORE_EXECUTION :
                case REJECTED_SEMAPHORE_FALLBACK :
                    return new RejectedExecutionException(failureType.toString(), cause);
            }

            return cause;
        }

        return error;
    }
}
