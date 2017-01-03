package org.zalando.rxnakadi.gson;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import java.util.Optional;
import java.util.function.Function;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Build Gson TypeAdapters in a functional way.
 */
public final class TypeAdapters {

    public interface Provider {
        <T> Optional<TypeAdapter<T>> get(TypeToken<T> type);

        default <T> Optional<TypeAdapter<T>> get(final Class<T> clazz) {
            return get(TypeToken.get(clazz));
        }

        default <T> TypeAdapter<T> require(final TypeToken<T> type) {
            return get(type).orElseThrow(() -> new IllegalArgumentException("No type adapter for " + type));
        }

        default <T> TypeAdapter<T> require(final Class<T> clazz) {
            return require(TypeToken.get(clazz));
        }

        static Provider of(final Gson gson) {
            return new Provider() {
                @Override
                public <T> Optional<TypeAdapter<T>> get(final TypeToken<T> type) {
                    return Optional.ofNullable(gson.getAdapter(type));
                }
            };
        }
    }

    /**
     * Writes objects to a {@link JsonWriter}.
     *
     * @param  <T>  the type of objects being written
     *
     * @see    TypeAdapter#read(JsonReader)
     */
    @FunctionalInterface
    public interface Writer<T> {
        void write(final JsonWriter out, final T value) throws IOException;

        static <T> Writer<T> toStringValue() {
            return (out, value) -> out.value(value.toString());
        }
    }

    /**
     * Reads objects from a {@code JsonReader}.
     *
     * @param  <T>  the type of objects being read
     *
     * @see    TypeAdapter#write(JsonWriter, Object)
     */
    @FunctionalInterface
    public interface Reader<T> {
        T read(JsonReader in) throws IOException;

        static <T> Reader<T> fromString(final Function<? super String, ? extends T> parser) {
            return in -> parser.apply(in.nextString());
        }
    }

    /**
     * Composes a {@code TypeAdapter} out of a {@link Reader} and a {@code Writer}.
     *
     * @param   reader  used to read objects from a JSON stream
     * @param   writer  used to write objects to a JSON stream
     * @param   <T>     the type of objects being converted
     *
     * @return  a {@code TypeAdapter} that uses the provided {@code reader} and {@code writer} to convert between
     *          objects and JSON
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    public static <T> TypeAdapter<T> of(final Reader<? extends T> reader, final Writer<? super T> writer) {
        requireNonNull(reader);
        requireNonNull(writer);

        return new TypeAdapter<T>() {
            @Override
            public T read(final JsonReader in) throws IOException {
                return reader.read(in);
            }

            @Override
            public void write(final JsonWriter out, final T value) throws IOException {
                writer.write(out, value);
            }
        };
    }

    private TypeAdapters() {
        throw new AssertionError("No instances for you!");
    }
}
