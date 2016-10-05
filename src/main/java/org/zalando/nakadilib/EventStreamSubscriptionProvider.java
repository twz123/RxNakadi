package org.zalando.nakadilib;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.net.URI;

import java.util.Collections;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.nakadilib.http.resource.Problem;

import org.zalando.undertaking.oauth2.AccessToken;
import org.zalando.undertaking.request.RequestProvider;
import org.zalando.undertaking.utils.FixedAttemptsStrategy;

import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

import io.undertow.util.StatusCodes;

import rx.Observable;
import rx.Single;

final class EventStreamSubscriptionProvider extends RequestProvider {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamSubscriptionProvider.class);

    private static final HystrixObservableCommand.Setter SETTER =
        HystrixObservableCommand.Setter.withGroupKey(                                                              //
                                           HystrixCommandGroupKey.Factory.asKey("nakadi"))                         //
                                       .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                                               .withExecutionTimeoutInMilliseconds(10000))                         //
                                       .andCommandKey(HystrixCommandKey.Factory.asKey("createSubscription"));

    private final Single<AccessToken> accessToken;

    @Inject
    public EventStreamSubscriptionProvider(final AsyncHttpClient httpClient, final Single<AccessToken> accessToken,
            final Gson gson) {
        super(httpClient, gson);
        this.accessToken = requireNonNull(accessToken);
    }

    public Single<NakadiSubscription> get(final URI nakadiUrl, final String owningApplication,
            final EventType eventType, final String consumerGroup) {

        final NakadiSubscription nakadiSubscription = //
            new NakadiSubscription(owningApplication, Collections.singleton(eventType), consumerGroup);

        return accessToken.flatMap(token -> buildRequest(nakadiUrl, token, nakadiSubscription))                    //
                          .flatMapObservable(requestBuilder ->
                                  Observable.defer(() -> createHystrixObservable(requestBuilder)))                 //
                          .doOnError(error ->
                                  LOG.warn("Subscription request failed: [{}]", error.toString()))                 //
                          .retry((attempt, error) ->
                                  new FixedAttemptsStrategy(3).shouldBeRetried(attempt, error))                    //
                          .onErrorResumeNext(error -> Observable.error(unwrapError(error)))                        //
                          .doOnError(error ->
                                  LOG.warn("No further retries for subscription request: [{}]", error.toString())) //
                          .toSingle();
    }

    private Single<BoundRequestBuilder> buildRequest(final URI nakadiUrl, final AccessToken token,
            final NakadiSubscription nakadiSubscription) {
        return Single.just(
                httpClient.preparePost(nakadiUrl + "/subscriptions")            //
                .setHeader(HttpHeaders.ACCEPT, "application/json")              //
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")        //
                .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token.getValue()) //
                .setBody(gson.toJson(nakadiSubscription)));
    }

    private Observable<NakadiSubscription> createHystrixObservable(final BoundRequestBuilder requestBuilder) {
        return new HystrixObservableCommand<NakadiSubscription>(SETTER) {
                @Override
                protected Observable<NakadiSubscription> construct() {
                    return
                        AsyncHttpSingle.create(requestBuilder)                                        //
                                       .flatMap(EventStreamSubscriptionProvider.this::handleResponse) //
                                       .toObservable();
                }
            }.toObservable();
    }

    private Single<NakadiSubscription> handleResponse(final Response response) {
        final int statusCode = response.getStatusCode();

        switch (statusCode) {

            case StatusCodes.OK :
            case StatusCodes.CREATED :
                return Single.just(gson.fromJson(response.getResponseBody(), NakadiSubscription.class));

            case StatusCodes.BAD_REQUEST :
            case StatusCodes.UNPROCESSABLE_ENTITY :

                final Problem problem = gson.fromJson(response.getResponseBody(), Problem.class);
                if (Strings.isNullOrEmpty(problem.getDetail())) {
                    final JsonElement errorDescription = gson.fromJson(response.getResponseBody(), JsonObject.class)
                                                             .get("error_description");
                    if (errorDescription != null) {
                        return Single.error(new HystrixBadRequestException(errorDescription.getAsString()));
                    }
                }

                return Single.error(new HystrixBadRequestException(problem.getDetail()));
        }

        return Single.error(new UnsupportedOperationException(
                    "Unsupported status code: " + statusCode + ": " + response.getResponseBody()));
    }

    private static Throwable unwrapError(final Throwable t) {
        if (t instanceof HystrixBadRequestException) {
            return firstNonNull(t.getCause(), t);
        } else if (t instanceof HystrixRuntimeException) {
            final Throwable cause = firstNonNull(t.getCause(), t);

            switch (((HystrixRuntimeException) t).getFailureType()) {

                case TIMEOUT :
                    return new TimeoutException(cause.getMessage()).initCause(cause);

                case REJECTED_SEMAPHORE_EXECUTION :
                case REJECTED_SEMAPHORE_FALLBACK :
                case REJECTED_THREAD_EXECUTION :
                case SHORTCIRCUIT :
                    return new RejectedExecutionException(cause.getMessage(), cause);

                default :
                    return cause;
            }
        }

        return t;
    }
}
