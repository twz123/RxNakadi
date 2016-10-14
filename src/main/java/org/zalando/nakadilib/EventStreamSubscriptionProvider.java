package org.zalando.nakadilib;

import static java.util.Objects.requireNonNull;

import static org.zalando.nakadilib.CursorCommitter.ERROR_CODES;
import static org.zalando.nakadilib.CursorCommitter.SUCCESS_CODES;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.net.URI;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.nakadilib.http.RequestProvider;
import org.zalando.nakadilib.http.resource.Problem;
import org.zalando.nakadilib.hystrix.HystrixCommands;
import org.zalando.nakadilib.oauth2.AccessToken;
import org.zalando.nakadilib.utils.FixedAttemptsStrategy;

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

        return accessToken.map(token -> buildRequest(nakadiUrl, token, nakadiSubscription))
        .flatMap(requestBuilder ->
              HystrixCommands.withRetries(() -> createHystrixObservable(requestBuilder),3)
        );
    }

    private BoundRequestBuilder buildRequest(final URI nakadiUrl, final AccessToken token,
            final NakadiSubscription nakadiSubscription) {
        return
                httpClient.preparePost(nakadiUrl + "/subscriptions")            //
                .setHeader(HttpHeaders.ACCEPT, "application/json")              //
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")        //
                .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token.getValue()) //
                .setBody(gson.toJson(nakadiSubscription));
    }

    private HystrixObservableCommand<NakadiSubscription> createHystrixObservable(final BoundRequestBuilder requestBuilder) {
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

        if (SUCCESS_CODES.contains(statusCode)) {
            return Single.just(gson.fromJson(response.getResponseBody(), NakadiSubscription.class));
        }

        if (ERROR_CODES.contains(statusCode)) {
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
