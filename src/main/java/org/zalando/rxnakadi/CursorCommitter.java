package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.URI;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.http.resource.Problem;
import org.zalando.rxnakadi.oauth2.AccessToken;
import org.zalando.rxnakadi.utils.StatusCodes;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;

import com.google.gson.Gson;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;

import rx.Observable;
import rx.Single;
import rx.Subscription;

import rx.functions.Func1;

final class CursorCommitter {

    /**
     * Logging instance of this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CursorCommitter.class);

    private static final HystrixObservableCommand.Setter SETTER =
        HystrixObservableCommand.Setter.withGroupKey(                                                             //
                                           HystrixCommandGroupKey.Factory.asKey("nakadi"))                        //
                                       .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                                               .withExecutionTimeoutInMilliseconds(1000))                         //
                                       .andCommandKey(HystrixCommandKey.Factory.asKey("commitSubscription"));

    private final AsyncHttpClient httpClient;
    private final Single<AccessToken> accessToken;
    private final Gson gson;

    @Inject
    public CursorCommitter(final AsyncHttpClient httpClient, final Single<AccessToken> accessToken, final Gson gson) {
        this.httpClient = requireNonNull(httpClient);
        this.accessToken = requireNonNull(accessToken);
        this.gson = requireNonNull(gson);

    }

    public Subscription autoCommit(final URI nakadiUrl, final Single<String> sessionId,
            final Supplier<Optional<Object>> cursorSupplier, final NakadiSubscription subscription,
            final long commitDelayMillis) {

        requireNonNull(nakadiUrl);
        requireNonNull(sessionId);
        requireNonNull(cursorSupplier);
        requireNonNull(subscription);
        checkArgument(commitDelayMillis >= 0, "commitDelayMillis may not be negative: %s", commitDelayMillis);

        final Func1<Observable<?>, Observable<?>> delayHandler = redo ->
                redo.flatMap(next -> Observable.timer(commitDelayMillis, TimeUnit.MILLISECONDS));

        return Single.zip(accessToken, sessionId, Single.fromCallable(cursorSupplier::get),
                         (token, id, cursor) -> {
                             final BoundRequestBuilder requestBuilder =                         //
                                 buildRequest(nakadiUrl, token, id, cursor, subscription.getId());

                             return Observable.defer(() ->
                                         new HystrixObservableCommand<CommitResult>(SETTER) {
                                             @Override
                                             protected Observable<CommitResult> construct() {
                                                 return
                                                     AsyncHttpSingle.create(requestBuilder)                      //
                                                                    .flatMapObservable(
                                                                        CursorCommitter.this::parseResponse);
                                             }
                                         }.toObservable().doOnNext(result -> {
                                             if (result != null
                                                     && CommitResult.RESULT_OUTDATED.equals(result.getResult())) {
                                                 LOG.warn("Committed cursor [{}] was already outdated.",
                                                     result.getCursor());
                                             }
                                         }))                                                                     //
                                 .doOnSubscribe(() -> LOG.debug("Committing cursor [{}].", cursor))              //
                                 .doOnCompleted(() -> LOG.info("Committed cursor [{}].", cursor))                //
                                 .doOnError(e ->
                                         LOG.info("Failed to commit cursor [{}]: [{}]", cursor, e.getMessage(), e));
                         }).flatMapObservable(observable -> observable)                                          //
                     .repeatWhen(delayHandler)                                                                   //
                     .retryWhen(delayHandler)                                                                    //
                     .doOnSubscribe(() ->
                             LOG.info("Auto commit cursors for event types [{}] every [{}ms].",
                                 subscription.getEventTypes(), commitDelayMillis))                               //
                     .doOnUnsubscribe(() ->
                             LOG.info("Unsubscribed from auto committing cursors for event types [{}].",
                                 subscription.getEventTypes()))                                                  //
                     .doOnTerminate(() ->
                             LOG.info("Ending auto committing cursors for events type [{}].",
                                 subscription.getEventTypes()))                                                  //
                     .subscribe();
    }

    private Observable<CommitResult> parseResponse(final Response response) {
        final int statusCode = response.getStatusCode();
        if (StatusCodes.SUCCESS_CODES.contains(statusCode)) {
            return Observable.just(gson.fromJson(response.getResponseBody(), CommitResult.class));
        }

        if (StatusCodes.ERROR_CODES.contains(statusCode)) {
            final Problem problem = gson.fromJson(response.getResponseBody(), Problem.class);
            return Observable.error(new HystrixBadRequestException(problem.getDetail()));
        }

        return Observable.error(new UnsupportedOperationException(
                    String.format("Unsupported status code: %s: %s", statusCode, response.getResponseBody())));

    }

    private BoundRequestBuilder buildRequest(final URI nakadiUrl, final AccessToken token, final String sessionId,
            final Optional<Object> cursor, final String subscriptionId) {
        final String commitUrl = String.format("%s/subscriptions/%s/cursors", nakadiUrl, subscriptionId);
        final CommitRequestPayload payload = new CommitRequestPayload(cursor.map(Collections::singleton).orElse(
                    Collections.emptySet()));

        return
            httpClient.preparePost(commitUrl)                                                   //
                      .setHeader(EventStreamHandler.NAKADI_CLIENT_IDENTIFIER_HEADER, sessionId) //
                      .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token.getValue())       //
                      .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")                  //
                      .setHeader(HttpHeaders.ACCEPT, "application/json")                        //
                      .setBody(gson.toJson(payload));
    }

    /**
     * Wrapper object containing the list of cursors being commited.
     */
    private static class CommitRequestPayload {
        final Set<Object> items;

        public CommitRequestPayload(final Set<Object> items) {
            this.items = ImmutableSet.copyOf(items);
        }
    }

    /**
     * The result of single cursor commit.
     *
     * <p>Holds a cursor itself and a result value.</p>
     */
    static class CommitResult {

        /**
         * Cursor was successfully committed.
         */
        static final String RESULT_SUCCESS = "committed";

        /**
         * There already was more recent (or the same) cursor committed, so the current one was not committed as it is
         * outdated.
         */
        static final String RESULT_OUTDATED = "outdated";

        private Object cursor;

        private String result;

        /**
         * Retrieves the cursor, which was tried to be commited.
         */
        public Object getCursor() {
            return cursor;
        }

        /**
         * Retrieves the result of cursor commit.
         *
         * <p>Possible result values are:</p>
         *
         * <ul>
         *   <li>{@link #RESULT_SUCCESS}</li>
         *   <li>{@link #RESULT_OUTDATED}</li>
         * </ul>
         */
        public String getResult() {
            return result;
        }
    }
}
