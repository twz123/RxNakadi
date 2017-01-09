package org.zalando.rxnakadi.http;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

import java.net.URI;

import java.util.Arrays;
import java.util.Map;

import org.hamcrest.Matcher;

import org.junit.Rule;
import org.junit.Test;

import org.zalando.rxnakadi.AccessToken;
import org.zalando.rxnakadi.NakadiPublishingException;
import org.zalando.rxnakadi.domain.BatchItemResponse;
import org.zalando.rxnakadi.domain.EventType;
import org.zalando.rxnakadi.gson.GsonJsonCoder;
import org.zalando.rxnakadi.gson.TypeAdapters;
import org.zalando.rxnakadi.internal.JsonCoder;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import com.google.gson.Gson;

import rx.Single;

import rx.observers.TestSubscriber;

/**
 * Abstract test that verifies the general contract of {@link NakadiHttpClient}.
 */
public abstract class NakadiHttpClientIT {

    @Rule
    public WireMockRule wireMock = new WireMockRule(options().dynamicPort());

    private Single<AccessToken> accessToken;

    private final JsonCoder json = new GsonJsonCoder(TypeAdapters.Provider.of(new Gson()));

    protected final URI nakadiUri() {
        return URI.create("http://localhost:" + wireMock.port());
    }

    protected final Single<AccessToken> accessToken() {
        return Single.defer(() -> accessToken);
    }

    protected final JsonCoder json() {
        return json;
    }

    protected abstract NakadiHttpClient underTest();

    @Test
    public void propagatesPublishingProblem() {
        accessToken = Single.just(AccessToken.bearer("i-will-fail"));
        wireMock.stubFor(
            post(urlEqualTo("/event-types/publish%20fails/events"))                          //
            .withHeader(AUTHORIZATION, equalTo("Bearer i-will-fail"))                        //
            .withHeader(ACCEPT, equalTo("application/json"))                                 //
            .withHeader(CONTENT_TYPE, matching("^application/json; *charset=(UTF|utf)-?8$")) //
            .withRequestBody(equalToJson("[1,2]"))                                           //
            .willReturn(
                aResponse()                                                                  //
                .withStatus(422)                                                             //
                .withStatusMessage("Unprocessable Entity")                                   //
                .withHeader(CONTENT_TYPE, "application/json;charset=UTF-8")                  //
                .withHeader("X-Flow-Id", "outa-flow")                                        //
                .withBody(new Gson().toJson(
                        Arrays.asList(                                                       //
                            problem("1", "", "failed", "validating"),                        //
                            problem("2", "", "aborted", "none"))))));

        final TestSubscriber<Void> subscriber = new TestSubscriber<>();
        underTest().publishEvents(EventType.of("publish fails"), Ints.asList(1, 2)).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoValues();
        assertThat(subscriber.getOnErrorEvents(), contains(instanceOf(NakadiPublishingException.class)));

        final NakadiPublishingException x = (NakadiPublishingException) subscriber.getOnErrorEvents().get(0);

        assertThat(x.getMessage(),
            is("2 events of type 'publish fails' could not be published to Nakadi. (Flow-ID: outa-flow)"));
        assertThat(x.getEventType(), hasToString("publish fails"));
        assertThat(x.getFlowId(), is("outa-flow"));
        assertThat(x.getProblems(),
            contains(                                            //
                problemMatcher("", "failed", "validating", "1"), //
                problemMatcher("", "aborted", "none", "2"))      //
            );
    }

    private static Map<String, String> problem(final String detail, final String eid, final String publishingStatus,
            final String step) {
        return ImmutableMap.of(                        //
                "detail", detail,                      //
                "eid", eid,                            //
                "publishing_status", publishingStatus, //
                "step", step                           //
            );
    }

    private static Matcher<BatchItemResponse> problemMatcher(final String eid, final String publishingStatus,
            final String step, final String detail) {

        return allOf(                                                  //
                instanceOf(BatchItemResponse.class),                   //
                hasProperty("eid", is(eid)),                           //
                hasProperty("publishingStatus", is(publishingStatus)), //
                hasProperty("step", is(step)),                         //
                hasProperty("detail", is(detail))                      //
                );
    }
}
