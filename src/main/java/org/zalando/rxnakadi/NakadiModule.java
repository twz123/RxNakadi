package org.zalando.rxnakadi;

import java.util.EnumSet;

import javax.inject.Singleton;

import org.zalando.rxnakadi.http.NakadiHttpClient;

import com.google.gson.Gson;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

@SuppressWarnings("static-method")
public final class NakadiModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(NakadiStreamProvider.class).in(Singleton.class);
        bind(NakadiPublisher.class).in(Singleton.class);
        bind(NakadiHttpClient.class);

        expose(NakadiStreamProvider.class);
        expose(NakadiPublisher.class);
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
