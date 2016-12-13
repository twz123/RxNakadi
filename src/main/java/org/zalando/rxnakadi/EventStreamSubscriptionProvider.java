package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.net.URI;

import java.text.MessageFormat;

import java.util.Collections;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.zalando.rxnakadi.http.RequestProvider;
import org.zalando.rxnakadi.http.resource.Problem;
import org.zalando.rxnakadi.hystrix.HystrixCommands;
import org.zalando.rxnakadi.utils.StatusCodes;

import org.zalando.undertaking.oauth2.AccessToken;

import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.netflix.hystrix.*;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

import rx.Observable;
import rx.Single;

final class EventStreamSubscriptionProvider extends RequestProvider {

    private static final HystrixObservableCommand.Setter SETTER =
        HystrixObservableCommand.Setter.withGroupKey(                                                             //
                                           HystrixCommandGroupKey.Factory.asKey("nakadi"))                        //
                                       .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                                               .withExecutionTimeoutInMilliseconds(1000))                         //
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

        return accessToken.map(token -> buildRequest(nakadiUrl, token, nakadiSubscription)).flatMap(requestBuilder ->
                    HystrixCommands.withRetries(() -> { return createHystrixObservable(requestBuilder); }, 3));
    }

    private BoundRequestBuilder buildRequest(final URI nakadiUrl, final AccessToken token,
            final NakadiSubscription nakadiSubscription) {
        return
            httpClient.preparePost(nakadiUrl + "/subscriptions")                          //
                      .setHeader(HttpHeaders.ACCEPT, "application/json")                  //
                      .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")            //
                      .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token.getValue()) //
                      .setBody(gson.toJson(nakadiSubscription));
    }

    private HystrixObservableCommand<NakadiSubscription> createHystrixObservable(
            final BoundRequestBuilder requestBuilder) {
        return new HystrixObservableCommand<NakadiSubscription>(SETTER) {
            @Override
            protected Observable<NakadiSubscription> construct() {
                return
                    AsyncHttpSingle.create(requestBuilder)                                        //
                                   .flatMap(EventStreamSubscriptionProvider.this::handleResponse) //
                                   .toObservable();
            }
        };
    }

    private Single<NakadiSubscription> handleResponse(final Response response) {
        final int statusCode = response.getStatusCode();

        if (StatusCodes.SUCCESS_CODES.contains(statusCode)) {
            return Single.just(gson.fromJson(response.getResponseBody(), NakadiSubscription.class));
        }

        if (StatusCodes.ERROR_CODES.contains(statusCode)) {
            final Problem problem = gson.fromJson(response.getResponseBody(), Problem.class);
            if (Strings.isNullOrEmpty(problem.getDetail())) {
                final JsonElement errorDescription = gson.fromJson(response.getResponseBody(), JsonObject.class).get(
                        "error_description");
                if (errorDescription != null) {
                    return Single.error(new HystrixBadRequestException(errorDescription.getAsString()));
                }
            }

            return Single.error(new HystrixBadRequestException(problem.getDetail()));
        }

        return Single.error(new UnsupportedOperationException(
                    MessageFormat.format("Unsupported status code: {0}: {1}", statusCode, response.getResponseBody())));
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
