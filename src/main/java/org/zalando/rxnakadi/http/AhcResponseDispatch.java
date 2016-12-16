package org.zalando.rxnakadi.http;

import java.util.List;

import org.asynchttpclient.Response;

import org.zalando.rxnakadi.rx.dispatch.Dispatcher;
import org.zalando.rxnakadi.rx.dispatch.IterableSelector;
import org.zalando.rxnakadi.rx.dispatch.Route;
import org.zalando.rxnakadi.rx.dispatch.Selector;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.net.MediaType;

import rx.Single;

public final class AhcResponseDispatch {

    public static Dispatcher<Response, Integer> statusCode() {
        return statusCodeDispatcher;
    }

    public static Dispatcher<Response, MediaType> contentType() {
        return contentTypeDispatcher;
    }

    public static Selector<Integer> onClientError() {
        return onClientError;
    }

    private static final Dispatcher<Response, Integer> statusCodeDispatcher = new Dispatcher<Response, Integer>() {
        @Override
        public String toString() {
            return "AhcResponseDispatch.statusCode()";
        }

        @Override
        public <R> Single<? extends R> dispatch(final Response response,
                final List<Route<Response, ? super Integer, R>> routes) {

            final Integer statusCode = response.getStatusCode();
            for (final Route<Response, ? super Integer, R> route : routes) {
                if (route.selector().test(statusCode)) {
                    return route.apply(response);
                }
            }

            return Single.error(new UnsupportedOperationException(
                        String.format("Unsupported %s %s: %s", "status code", statusCode, responseString(response))));
        }
    };

    private static final Dispatcher<Response, MediaType> contentTypeDispatcher = new Dispatcher<Response, MediaType>() {
        @Override
        public String toString() {
            return "AhcResponseDispatch.contentType()";
        }

        @Override
        public <R> rx.Single<? extends R> dispatch(final Response response,
                final List<Route<Response, ? super MediaType, R>> routes) {
            final String contentTypeString = response.getContentType();
            final MediaType contentType;
            try {
                contentType = contentTypeString == null ? null : MediaType.parse(contentTypeString);
            } catch (final IllegalArgumentException e) {
                final String msg = String.format("Failed to parse content type %s: %s", //
                        contentTypeString, responseString(response));
                return Single.error(new IllegalArgumentException(msg, e));
            }

            iterateRoutes:
            for (final Route<Response, ? super MediaType, R> route : routes) {
                final Selector<? super MediaType> selector = route.selector();

                checkAttributes:
                if (selector instanceof IterableSelector) {
                    final IterableSelector<? super MediaType> iterable = (IterableSelector<? super MediaType>) selector;

                    for (final Object attribute : iterable) {
                        if (attribute instanceof MediaType) {
                            if (contentType.is((MediaType) attribute)) {
                                return route.apply(response);
                            }
                        } else {
                            break checkAttributes;
                        }
                    }

                    continue iterateRoutes;
                }

                if (selector.test(contentType)) {
                    return route.apply(response);
                }
            }

            return Single.error(new UnsupportedOperationException(
                        String.format("Unsupported content type %s: %s", contentType, responseString(response))));
        }
    };

    private static final Selector<Integer> onClientError = new IterableSelector.AttributeSet<Integer>( //
            ContiguousSet.create(Range.closed(400, 499), DiscreteDomain.integers())) {
        @Override
        public String toString() {
            return "AhcResponseDispatch.onClientError()";
        }
    };

    static String responseString(final Response response) {
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

    private AhcResponseDispatch() {
        throw new AssertionError("No instances for you!");
    }
}
