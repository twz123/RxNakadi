package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import java.util.EnumSet;
import java.util.function.Consumer;

import javax.inject.Singleton;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

import net.dongliu.gson.GsonJava8TypeAdapterFactory;

@SuppressWarnings("static-method")
public final class NakadiModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(NakadiStreamProvider.class).in(Singleton.class);
        bind(NakadiPublisher.class).in(Singleton.class);
        bind(EventStreamSubscriptionProvider.class);
        bind(CursorCommitter.class);

        expose(NakadiStreamProvider.class);
        expose(NakadiPublisher.class);
    }

    @Provides
    @Internal
    AsyncHttpClient provideNakadiHttpClient() {
        final AsyncHttpClientConfig config =
                new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(-1) // disable waiting for request completion
                        .build();

        return new DefaultAsyncHttpClient(config);
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
