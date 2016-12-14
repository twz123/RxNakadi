package org.zalando.rxnakadi.http;

import static java.util.Objects.requireNonNull;

import static org.zalando.rxnakadi.http.DelimitedJsonStreamTarget.JSON_STREAM_TYPE;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.extras.rxjava.single.AsyncHttpSingle;

import org.zalando.rxnakadi.EventType;
import org.zalando.rxnakadi.NakadiSubscription;
import org.zalando.rxnakadi.hystrix.HystrixCommands;

import org.zalando.undertaking.oauth2.AccessToken;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;
import com.google.common.net.UrlEscapers;

import com.google.gson.Gson;

import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.exception.HystrixBadRequestException;

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

    private final AsyncHttpClient http;
    private String nakadiUrl;
    private final Single<AccessToken> accessToken;
    private final Gson gson;
    private HystrixCommandGroupKey groupKey;

    @Inject
    public NakadiHttpClient(final Single<AccessToken> accessToken, final AsyncHttpClient http, final Gson gson) {
        this.accessToken = requireNonNull(accessToken);
        this.http = requireNonNull(http);
        this.gson = requireNonNull(gson);
    }

    public Single<NakadiSubscription> getSubscription( //
            final EventType eventType, final String owningApplication, final String consumerGroup) {

        final String url = nakadiUrl + "/subscriptions";
        final String payload = gson.toJson(new NakadiSubscription( //
                    owningApplication, Collections.singleton(eventType), consumerGroup));

        return request("getSubscription", response -> gson.fromJson(response, NakadiSubscription.class),
                () ->
                    Dsl.post(url)                               //
                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString()) //
                    .setBody(payload));
    }

    public Observable<String> getEventsForType(final EventType eventType) {
        final String url = String.format("%s/subscriptions/%s/events", //
                nakadiUrl, escapePathSegment(eventType.toString()));
        return getEvents(url, null);
    }

    public Observable<String> getEventsForSubscription(final String subscriptionId,
            final Consumer<String> nakadiStreamId) {
        checkArgument(subscriptionId.isEmpty(), "subscriptionId may not be empty");

        final String url = String.format("%s/subscriptions/%s/events", //
                nakadiUrl, escapePathSegment(subscriptionId));
        return getEvents(url, nakadiStreamId);
    }

    public Single<String> commitCursor(final Optional<Object> cursor, final String subscriptionId,
            final String streamId) {
        final String url = String.format("%s/subscriptions/%s/cursors", nakadiUrl, escapePathSegment(subscriptionId));

        final String payload =                                   //
            cursor.map(Collections::singleton)                   //
                  .map(items -> ImmutableMap.of("items", items)) //
                  .map(gson::toJson)                             //
                  .orElse("[]");

        return request("commitCursor",
                response -> Objects.toString(gson.fromJson(response, Map.class).get("result"), null),
                () ->
                    Dsl.post(url)                               //
                    .setHeader(X_NAKADI_STREAM_ID, streamId)    //
                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString()) //
                    .setBody(payload));
    }

    private <T> Single<T> request(final String commandKeyName,
            final Function<? super String, ? extends T> responseParser,
            final Supplier<RequestBuilder> requestSupplier) {
        final HystrixObservableCommand.Setter setter =             //
            HystrixObservableCommand.Setter.withGroupKey(groupKey) //
                                           .andCommandKey(HystrixCommandKey.Factory.asKey(commandKeyName));
        return HystrixCommands.withRetries(() -> toHystrixCommand(setter, requestSupplier, responseParser), 3);
    }

    private <T> Observable<T> request(                      //
            final Supplier<RequestBuilder> requestSupplier, //
            final Function<? super String, ? extends T> responseParser) {

        return accessToken.map(token -> {
                              final RequestBuilder builder = requestSupplier.get();
                              builder.setHeader(AUTHORIZATION, token.getTypeAndValue());
                              builder.setHeader(ACCEPT, jsonType.toString());
                              builder.addHeader(ACCEPT, starJsonType.toString());
                              return builder.build();
                          })                                                   //
                          .flatMap(requestWithAuth -> {
                              return AsyncHttpSingle.create(handler ->
                                          http.executeRequest(requestWithAuth, handler));
                          })                                                   //
                          .<T>map(response -> parse(response, responseParser)) //
                          .toObservable();
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

    private <T> HystrixObservableCommand<T> toHystrixCommand( //
            final HystrixObservableCommand.Setter setter, final Supplier<RequestBuilder> requestSupplier,
            final Function<? super String, ? extends T> responseParser) {
        return new HystrixObservableCommand<T>(setter) {
            @Override
            protected Observable<T> construct() {
                return request(requestSupplier, responseParser);
            }
        };
    }

    private Observable<String> getEvents(final String url, final Consumer<String> nakadiStreamId) {
        return accessToken.flatMapObservable(token -> {
                final Request request =
                    Dsl.get(url)                                   //
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

    private static String responseString(final Response response) {
        final String responseBody = response.getResponseBody();
        final int numBodyChars = responseBody.length();
        final StringBuilder buf = new StringBuilder(64 + Math.min(numBodyChars, 1000));

        buf.append(response.getStatusCode()).append(' ').append(response.getStatusText());
        if (numBodyChars > 0) {
            buf.append(": ");
            if (numBodyChars > 1000) {
                buf.append(responseBody, 0, 999).append('…');
            } else {
                buf.append(responseBody);
            }
        }

        return buf.toString();
    }

    private static String escapePathSegment(final String pathSegment) {
        return UrlEscapers.urlPathSegmentEscaper().escape(pathSegment);
    }
}
