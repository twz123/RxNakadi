package org.zalando.rxnakadi.gson;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import org.zalando.rxnakadi.internal.JsonCoder;

import com.google.common.reflect.TypeToken;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Gson implementation of {@code JsonCoder}, handling RxNakadi internal types, delegating everything else to user
 * provided {@code TypeAdapters}.
 *
 * @see  InternalFactory
 */
public final class GsonJsonCoder implements JsonCoder {

    private final Gson gson;

    public GsonJsonCoder(final TypeAdapters.Provider userAdapters) {
        gson =
            new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES)             //
                             .registerTypeAdapterFactory(new GuavaTypeAdapterFactory())     //
                             .registerTypeAdapterFactory(new InternalFactory(userAdapters)) //
                             .create();
    }

    @Override
    public <T> T fromJson(final String json, final TypeToken<T> type) {
        return gson.fromJson(json, type.getType());
    }

    @Override
    public String toJson(final Object value) {
        return gson.toJson(value);
    }
}
