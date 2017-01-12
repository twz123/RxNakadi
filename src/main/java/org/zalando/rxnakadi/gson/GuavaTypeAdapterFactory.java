package org.zalando.rxnakadi.gson;

import java.io.IOException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.function.Supplier;

import org.zalando.rxnakadi.gson.TypeAdapters.Reader;
import org.zalando.rxnakadi.gson.TypeAdapters.Writer;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

/**
 * TypeAdapters for internally used Guava classes.
 */
final class GuavaTypeAdapterFactory implements TypeAdapterFactory {

    @Override
    public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> type) {
        final Class<? super T> rawType = type.getRawType();

        if (rawType == ImmutableList.class || rawType == ImmutableCollection.class) {
            return createAdapter(gson, type, ImmutableList::builder);
        }

        if (rawType == ImmutableSet.class) {
            return createAdapter(gson, type, ImmutableSet::builder);
        }

        return null;
    }

    private <T> TypeAdapter<T> createAdapter( //
            final Gson gson, final TypeToken<T> type,
            final Supplier<? extends ImmutableCollection.Builder<Object>> builder) {

        final TypeAdapter<?> elementAdapter = gson.getAdapter(getElementType(type.getType()));

        @SuppressWarnings("unchecked")
        final Reader<T> reader = in -> (T) readCollection(in, type, builder.get(), elementAdapter);
        return TypeAdapters.of(reader, Writer.of(gson.getDelegateAdapter(this, type)));
    }

    private static <E, B extends ImmutableCollection.Builder<? super E>> ImmutableCollection<? super E> readCollection( //
            final JsonReader in, final TypeToken<?> type, final B builder,
            final TypeAdapter<? extends E> elementAdapter) throws IOException {

        if (in.peek() == JsonToken.NULL) {
            in.nextNull();
            return null;
        }

        in.beginArray();

        while (in.peek() != JsonToken.END_ARRAY) {
            final E value = elementAdapter.read(in);
            if (value == null) {
                throw new JsonSyntaxException("Encountered a null array element while reading an instance of type "
                        + type + " at " + in.getPath());
            }

            builder.add(value);
        }

        in.endArray();

        return builder.build();
    }

    private static TypeToken<?> getElementType(final Type type) {
        if (type instanceof ParameterizedType) {
            final Type[] actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
            return TypeToken.get(actualTypeArguments[0]);
        }

        return TypeToken.get(Object.class);
    }
}
