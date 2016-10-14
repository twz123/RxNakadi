package org.zalando.nakadilib.http;

import static java.util.Objects.requireNonNull;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import org.asynchttpclient.AsyncHttpClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Allows child classes to create requests using {@code AsyncHttpClient} and JSON parsing via {@code Gson}.
 */
public abstract class RequestProvider {

    protected final AsyncHttpClient httpClient;
    protected final Gson gson;

    public RequestProvider(final AsyncHttpClient httpClient) {
        this(httpClient, new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create());
    }

    public RequestProvider(final AsyncHttpClient httpClient, final Gson gson) {
        this.httpClient = requireNonNull(httpClient);
        this.gson = requireNonNull(gson);
    }
}
