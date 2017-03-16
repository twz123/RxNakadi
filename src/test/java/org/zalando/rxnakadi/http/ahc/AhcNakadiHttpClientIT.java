package org.zalando.rxnakadi.http.ahc;

import static org.asynchttpclient.Dsl.asyncHttpClient;

import java.io.IOException;

import org.asynchttpclient.AsyncHttpClient;

import org.junit.After;
import org.junit.Before;

import org.zalando.rxnakadi.http.NakadiHttpClient;
import org.zalando.rxnakadi.http.NakadiHttpClientIT;

import io.github.robwin.circuitbreaker.CircuitBreakerRegistry;

public class AhcNakadiHttpClientIT extends NakadiHttpClientIT {

    private AsyncHttpClient http;

    private AhcNakadiHttpClient underTest;

    @Before
    public void initializeTest() {
        http = asyncHttpClient();
        underTest = new AhcNakadiHttpClient(CircuitBreakerRegistry.ofDefaults(), nakadiUri(), http, accessToken(),
                json());
    }

    @After
    public void finalizeTest() throws IOException {
        if (http != null) {
            http.close();
            http = null;
        }
    }

    @Override
    protected NakadiHttpClient underTest() {
        return underTest;
    }

}
