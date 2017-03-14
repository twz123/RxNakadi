package org.zalando.rxnakadi.rx.dispatch;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.contentType;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.onClientError;
import static org.zalando.rxnakadi.http.ahc.AhcResponseDispatch.statusCode;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.dispatch;
import static org.zalando.rxnakadi.rx.dispatch.RxDispatch.on;

import java.util.Iterator;

import org.asynchttpclient.Response;

import org.hamcrest.Matcher;

import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.net.MediaType;

import io.reactivex.Single;

public class RxDispatchTest {

    @Test
    public void typicalUseCase() {

        final Iterator<Response> responses = Iterators.forArray( //
                mockResponse(200, null),                         //
                mockResponse(200, "text/html"),                  //
                mockResponse(200, "application/json"),           //
                mockResponse(200, "application/x-foo+json"),     //
                mockResponse(400, null),                         //
                mockResponse(500, null)                          //
                );

        final Single<String> testSetup =
            Single.fromCallable(responses::next)                                                //
                  .flatMap(dispatch(statusCode(),                                               //
                          on(200).dispatch(contentType(),                                       //
                              on(MediaType.create("application", "json")).map(response -> "OK json"), //
                              on(MediaType.create("application", "*")).map(response -> "OK star")), //
                          onClientError().map(r -> "Error")));

        verifyDispatch(testSetup,
            allOf(instanceOf(UnsupportedOperationException.class),
                hasProperty("message", is("No content type: 200 null: 200 null"))));
        verifyDispatch(testSetup,
            allOf(instanceOf(UnsupportedOperationException.class),
                hasProperty("message", is("Unsupported content type text/html: 200 null: 200 text/html"))));
        verifyDispatch(testSetup, "OK json");
        verifyDispatch(testSetup, "OK star");
        verifyDispatch(testSetup, "Error");
        verifyDispatch(testSetup,
            allOf(instanceOf(UnsupportedOperationException.class),
                hasProperty("message", is("Unsupported status code 500: 500 null: 500 null"))));
    }

    private static void verifyDispatch(final Single<String> testSetup, final String value) {
        testSetup.test().assertComplete().assertNoErrors().assertValue(value);
    }

    private static void verifyDispatch(final Single<String> testSetup, final Matcher<? super Throwable> errorMatcher) {
        testSetup.test().assertTerminated().assertNoValues().assertError(errorMatcher::matches);
    }

    private static Response mockResponse(final int statusCode, final String contentType) {
        final Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(statusCode);
        when(response.getContentType()).thenReturn(contentType);
        when(response.getResponseBody()).thenReturn(statusCode + " " + contentType);
        return response;
    }
}
