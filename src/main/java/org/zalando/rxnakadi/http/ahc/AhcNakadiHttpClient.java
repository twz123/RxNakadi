package org.zalando.rxnakadi.http.ahc;

import static java.util.Objects.requireNonNull;

import static org.asynchttpclient.util.HttpConstants.Methods.POST;

import static org.zalando.rxnakadi.http.NakadiHttp.JSON_TYPE;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.contentType;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.onClientError;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.responseString;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.statusCode;
import static org.zalando.rxnakadi.internal.TypeTokens.listOf;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.dispatch;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.on;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.onAnyOf;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;

import java.net.URI;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava2.single.AsyncHttpSingle;

import org.asynchttpclient.uri.Uri;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.AccessToken;
import org.zalando.rxnakadi.NakadiPublishingException;
import org.zalando.rxnakadi.domain.BatchItemResponse;
import org.zalando.rxnakadi.domain.EventType;
import org.zalando.rxnakadi.http.NakadiHttp;
import org.zalando.rxnakadi.http.NakadiHttpClient;
import org.zalando.rxnakadi.inject.Nakadi;
import org.zalando.rxnakadi.internal.JsonCoder;

import com.google.common.collect.ImmutableSet;

import io.github.robwin.circuitbreaker.CircuitBreaker;
import io.github.robwin.circuitbreaker.CircuitBreakerConfig;
import io.github.robwin.circuitbreaker.CircuitBreakerRegistry;
import io.github.robwin.circuitbreaker.operator.CircuitBreakerOperator;

import io.reactivex.Completable;
import io.reactivex.Single;

import io.reactivex.functions.BiPredicate;

public final class AhcNakadiHttpClient implements NakadiHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(AhcNakadiHttpClient.class);

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Uri nakadiEndpoint;
    private final AsyncHttpClient http;
    private final Single<AccessToken> accessToken;
    private final JsonCoder json;

    /**
     * List of exceptions being unrecoverable and which should not count for opening the Circuit Breaker.
     */
    private static final Set<Class<? extends Throwable>> UNRECOVERABLE_EXCEPTIONS = ImmutableSet.of(
            BadRequestException.class, NakadiPublishingException.class);

    @Inject
    AhcNakadiHttpClient(final CircuitBreakerRegistry circuitBreakerRegistry, @Nakadi final URI nakadiEndpoint,
            final AsyncHttpClient http, @Nakadi final Single<AccessToken> accessToken, final JsonCoder json) {
        this.circuitBreakerRegistry = requireNonNull(circuitBreakerRegistry);
        this.nakadiEndpoint = Uri.create(nakadiEndpoint.toString());
        this.http = requireNonNull(http);
        this.accessToken = requireNonNull(accessToken);
        this.json = requireNonNull(json);
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
                                    .setBody(json.toJson(events))                                    //
                                    .build();
        final CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("publishEvents",
                buildCircuitBreakerConfig());

        return http(request).flatMap(dispatch(statusCode(),                                            //
                                    on(200).dispatch(contentType(), on(JSON_TYPE).pass()),             //
                                    onAnyOf(207, 422).dispatch(contentType(),                          //
                                        on(JSON_TYPE).error(response ->
                                                publishingProblem(eventType, response))),              //
                                    onClientError().error(this::badRequest)))                          //
                            .retry(maxRetriesOr(3, exceptionIsNotOfType(UNRECOVERABLE_EXCEPTIONS)))
                            .timeout(2000L, TimeUnit.MILLISECONDS, Single.error(new TimeoutException())) //
                            .lift(CircuitBreakerOperator.of(circuitBreaker))                           //
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
                                  .doOnSubscribe(disposable ->
                                          LOG.debug("Request: [{} {}], headers: [{}], body: [{}]", request.getMethod(),
                                              request.getUri(), request.getHeaders().entries(),
                                              request.getStringData()));
                          })                                                                            //
                          .doOnSuccess(response ->
                                  LOG.debug("Response: [{} {}], headers: [{}], body: [{}]", response.getStatusCode(),
                                      response.getStatusText(), response.getHeaders().entries(),
                                      response.getResponseBody()));
    }

    private Uri buildUri(final String template, final String... pathParams) {
        final Object[] escapedPathParams = Stream.of(pathParams).map(urlPathSegmentEscaper()::escape).toArray();
        return Uri.create(nakadiEndpoint, String.format(template, escapedPathParams));
    }

    private CircuitBreakerConfig buildCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom().recordFailure(exceptionIsNotOfType(UNRECOVERABLE_EXCEPTIONS)) //
                                   .build();
    }

    private BiPredicate<Integer, Throwable> maxRetriesOr(final int maxRetries, final Predicate<Throwable> pred) {
        return (tries, ex) -> tries <= maxRetries && pred.test(ex);
    }

    private Predicate<Throwable> exceptionIsNotOfType(final Collection<Class<? extends Throwable>> exceptionTypes) {
        return e -> !exceptionTypes.contains(e.getClass());
    }

    private NakadiPublishingException publishingProblem(final EventType eventType, final Response response) {
        return new NakadiPublishingException(eventType, response.getHeader(NakadiHttp.X_FLOW_ID),
                json.fromJson(response.getResponseBody(), listOf(BatchItemResponse.class)));
    }

    private BadRequestException badRequest(final Response response) {
        return new BadRequestException(responseString(response));
    }
}
