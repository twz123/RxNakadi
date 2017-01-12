package org.zalando.rxnakadi.gson;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import java.util.function.Function;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * Build Gson {@link TypeAdapter TypeAdapters} in a functional way.
 */
public final class TypeAdapters {

    /**
     * Provides {@link TypeAdapter TypeAdapters}.
     */
    public interface Provider {

        /**
         * Returns the {@code TypeAdapter} for {@code type}.
         *
         * @param   type  the type for which to return the {@code TypeAdapter}
         *
         * @return  the {@code TypeAdapter} for {@code type}
         *
         * @throws  NullPointerException      if {@code type} is {@code null}
         * @throws  IllegalArgumentException  if no {@code TypeAdapter} was found
         */
        <T> TypeAdapter<T> get(final TypeToken<T> type);

        /**
         * Returns the {@code TypeAdapter} for {@code clazz}.
         *
         * @param   clazz  the class for which to return the {@code TypeAdapter}
         *
         * @return  the {@code TypeAdapter} for {@code clazz}
         *
         * @throws  NullPointerException      if {@code clazz} is {@code null}
         * @throws  IllegalArgumentException  if no {@code TypeAdapter} was found
         */
        default <T> TypeAdapter<T> get(final Class<T> clazz) {
            return get(TypeToken.get(clazz));
        }

        /**
         * Delegates {@code TypeAdapter} lookups to {@link Gson#getAdapter(TypeToken) Gson}.
         *
         * @param   gson  the Gson instance to be used for lookups
         *
         * @return  a {@code Provider} that wraps {@code gson}
         *
         * @throws  NullPointerException  if {@code gson} is {@code null}
         */
        static Provider of(final Gson gson) {
            requireNonNull(gson);

            return new Provider() {
                @Override
                public <T> TypeAdapter<T> get(final TypeToken<T> type) {
                    return gson.getAdapter(requireNonNull(type));
                }
            };
        }
    }

    /**
     * Writes objects to a {@link JsonWriter}.
     *
     * @param  <T>  the type of objects being written
     */
    @FunctionalInterface
    public interface Writer<T> {

        /**
         * Writes one JSON value (an array, object, string, number, boolean or null) for {@code value}.
         *
         * @param  out    the {@code JsonWriter} to which {@code value} is written to
         * @param  value  the object to write
         *
         * @see    TypeAdapter#write(JsonWriter, T)
         */
        void write(final JsonWriter out, final T value) throws IOException;

        /**
         * Returns the {@link TypeAdapter#write(JsonWriter, T) write logic part} of {@code typeAdapter}.
         *
         * @param   typeAdapter  the {@code TypeAdapter} from which to take the write logic.
         *
         * @return  a {@code Writer} that uses the write logic of {@code typeAdapter}
         *
         * @throws  NullPointerException  if {@code typeAdapter} is {@code null}
         */
        static <T> Writer<? super T> of(final TypeAdapter<T> typeAdapter) {
            requireNonNull(typeAdapter);
            return typeAdapter instanceof ComposedAdapter ? ((ComposedAdapter<T>) typeAdapter).writer()
                                                          : typeAdapter::write;
        }

        /**
         * @return  a {@code Writer} that writes strings
         */
        static Writer<String> writeString() {
            return new Writer<String>() {
                @Override
                public String toString() {
                    return "writeString()";
                }

                @Override
                public void write(final JsonWriter out, final String value) throws IOException {
                    out.value(value);
                }
            };
        }

        /**
         * Returns a {@code Writer} that switches the
         * {@link JsonWriter#setSerializeNulls(boolean) null-serialization mode} to the desired state when writing
         * values.
         *
         * @param   serializeNulls  whether object members are serialized when their value is {@code null} or not
         *
         * @return  a {@code Writer} that serializes {@code nulls} or not, depending on {@code serializeNulls}
         */
        default Writer<T> serializeNulls(final boolean serializeNulls) {
            return new Writer<T>() {
                @Override
                public String toString() {
                    return Writer.this.toString() + ".serializeNulls(" + serializeNulls + ')';
                }

                @Override
                public void write(final JsonWriter out, final T value) throws IOException {
                    final boolean oldSerializeNulls = out.getSerializeNulls();
                    if (oldSerializeNulls == serializeNulls) {
                        Writer.this.write(out, value);
                    } else {
                        out.setSerializeNulls(serializeNulls);
                        try {
                            Writer.this.write(out, value);
                        } finally {
                            out.setSerializeNulls(oldSerializeNulls);
                        }
                    }
                }
            };
        }

        /**
         * Returns a composed {@code Writer} that first applies the {@code before} function to its input, and then
         * writes the result.
         *
         * @param   <U>     the type of input to {@code before} and of values that can be written by the returned
         *                  {@code Writer}
         * @param   before  the function to apply to values before they are written
         *
         * @return  a composed {@code Writer} that first applies the {@code before} function and then writes the result
         *
         * @throws  NullPointerException  if {@code before} is {@code null}
         */
        default <U> Writer<U> compose(final Function<? super U, ? extends T> before) {
            requireNonNull(before);
            return (out, value) -> write(out, value == null ? null : before.apply(value));
        }
    }

    /**
     * Reads objects from a {@code JsonReader}.
     *
     * @param  <T>  the type of objects being read
     */
    @FunctionalInterface
    public interface Reader<T> {

        /**
         * Reads one JSON value (an array, object, string, number, boolean or null) and converts it to an object.
         *
         * @return  the object that has been read (may be {@code null})
         *
         * @see     TypeAdapter#read(JsonReader)
         */
        T read(JsonReader in) throws IOException;

        /**
         * Returns the {@link TypeAdapter#read(JsonReader)) read logic part} of {@code typeAdapter}.
         *
         * @param   typeAdapter  the {@code TypeAdapter} from which to take the read logic.
         *
         * @return  a {@code Reader} that uses the read logic of {@code typeAdapter}
         *
         * @throws  NullPointerException  if {@code typeAdapter} is {@code null}
         */
        static <T> Reader<? extends T> of(final TypeAdapter<T> typeAdapter) {
            requireNonNull(typeAdapter);
            return typeAdapter instanceof ComposedAdapter ? ((ComposedAdapter<T>) typeAdapter).reader()
                                                          : typeAdapter::read;
        }

        /**
         * @return  a {@code Reader} that reads a JSON string value
         */
        static Reader<String> readString() {
            return new Reader<String>() {
                @Override
                public String toString() {
                    return "readString()";
                }

                @Override
                public String read(final JsonReader in) throws IOException {
                    if (in.peek() == JsonToken.NULL) {
                        in.nextNull();
                        return null;
                    }

                    return in.nextString();
                }
            };
        }

        /**
         * Returns a composed {@code Reader} that first reads the value, and then applies the {@code after} function to
         * the read value, returning its result.
         *
         * @param   <V>    the type of output of the {@code after} function, and of the values being read by the
         *                 returned {@code Reader}.
         * @param   after  the function to apply after the value has been read
         *
         * @return  a composed {@code Reader} that first reads the value and then applies the {@code after} function to
         *          it
         *
         * @throws  NullPointerException  if {@code after} is {@code null}
         */
        default <U> Reader<U> andThen(final Function<? super T, ? extends U> after) {
            requireNonNull(after);
            return in -> {
                final T value = read(in);
                return value == null ? null : after.apply(value);
            };
        }
    }

    /**
     * A {@code TypeAdapter} that is composed of a {@link Reader} and a {@code Writer}.
     *
     * @param  <T>  the type of objects being converted
     */
    public abstract static class ComposedAdapter<T> extends TypeAdapter<T> {

        /**
         * @return  the underlying {@code Reader}
         */
        public abstract Reader<? extends T> reader();

        /**
         * @return  the underlying {@code Writer}
         */
        public abstract Writer<? super T> writer();

        @Override
        public T read(final JsonReader in) throws IOException {
            return reader().read(in);
        }

        @Override
        public void write(final JsonWriter out, final T value) throws IOException {
            writer().write(out, value);
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

        return new ComposedAdapter<T>() {
            @Override
            public String toString() {
                return String.format("TypeAdapters.of(%s, %s)", reader, writer);
            }

            @Override
            public Reader<? extends T> reader() {
                return reader;
            }

            @Override
            public Writer<? super T> writer() {
                return writer;
            }
        };
    }

    private TypeAdapters() {
        throw new AssertionError("No instances for you!");
    }
}
