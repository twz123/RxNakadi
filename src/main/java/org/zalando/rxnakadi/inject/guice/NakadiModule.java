package org.zalando.rxnakadi.inject.guice;

import java.net.URI;

import java.util.EnumSet;

import javax.inject.Singleton;

import org.asynchttpclient.AsyncHttpClient;

import org.zalando.rxnakadi.NakadiTopicFactory;
import org.zalando.rxnakadi.http.NakadiHttpClient;
import org.zalando.rxnakadi.http.ahc.AhcNakadiHttpClient;
import org.zalando.rxnakadi.inject.Nakadi;
import org.zalando.rxnakadi.internal.DefaultNakadiTopicFactory;

import org.zalando.undertaking.oauth2.AccessToken;

import com.google.gson.Gson;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

/**
 * Provides bindings for {@link NakadiTopicFactory}.
 *
 * <p>Required dependencies:
 *
 * <dl>
 *   <dt>{@code @}{@link Nakadi} {@link URI}</dt>
 *   <dd>The Nakadi HTTP endpoint.</dd>
 *
 *   <dt>{@code @}{@link Nakadi} {@link Single}{@code <}{@link AccessToken}{@code >}</dt>
 *   <dd>The access token to authenticate with Nakadi.</dd>
 *
 *   <dt>{@link Gson}</dt>
 *   <dd>To parse JSON from and produce JSON for Nakadi.</dd>
 *
 *   <dt>{@link AsyncHttpClient}</dt>
 *   <dd>HTTP transport provider.</dd>
 * </dl>
 * </p>
 */
@SuppressWarnings("static-method")
public final class NakadiModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(NakadiHttpClient.class).to(AhcNakadiHttpClient.class).in(Singleton.class);

        bind(NakadiTopicFactory.class).to(DefaultNakadiTopicFactory.class).in(Singleton.class);
        expose(NakadiTopicFactory.class);
    }

    @Provides
    @Singleton
    ParseContext provideJsonPathParseContext(final Gson gson) {
        return JsonPath.using(
                Configuration.builder()                     //
                .jsonProvider(new GsonJsonProvider(gson))   //
                .mappingProvider(new GsonMappingProvider(gson)) //
                .options(EnumSet.noneOf(Option.class))      //
                .build());
    }
}
