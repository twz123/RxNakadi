package org.zalando.rxnakadi.gson;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.time.Instant;

import org.zalando.rxnakadi.domain.Cursor;
import org.zalando.rxnakadi.domain.EventType;
import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.domain.StreamInfo;
import org.zalando.rxnakadi.gson.TypeAdapters.Reader;
import org.zalando.rxnakadi.gson.TypeAdapters.Writer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;

/**
 * Internal Gson {@code TypeAdapterFactory} that handles built-in RxNakadi types. Subtypes of {@link NakadiEvent} are
 * handled via user provided type adapters, except the metadata part.
 *
 * @see  GsonJsonCoder
 */
final class InternalFactory implements TypeAdapterFactory {
    private final TypeAdapters.Provider userAdapters;

    public InternalFactory(final TypeAdapters.Provider userAdapters) {
        this.userAdapters = requireNonNull(userAdapters);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> type) {
        final Class<? super T> rawType = type.getRawType();

        if (rawType == Instant.class) {
            return (TypeAdapter<T>) TypeAdapters.of(                                                 //
                    Reader.readString().andThen(ISO_OFFSET_DATE_TIME::parse).andThen(Instant::from), //
                    Writer.writeString().compose(Instant::toString));
        }

        if (rawType == EventType.class) {
            return (TypeAdapter<T>) TypeAdapters.of(            //
                    Reader.readString().andThen(EventType::of), //
                    Writer.writeString().compose(EventType::toString));
        }

        if (NakadiEvent.class.isAssignableFrom(rawType)) {
            return (TypeAdapter<T>) eventAdapter(gson, (TypeToken<? extends NakadiEvent>) type);
        }

        final Reader<? extends T> reader = getReader(gson, type);

        if (reader != null) {
            Writer<? super T> writer = getWriter(gson, type);
            if (writer == null) {
                writer = Writer.of(gson.getDelegateAdapter(this, type));
            }

            return TypeAdapters.<T>of(reader, writer);
        }

        final Writer<? super T> writer = getWriter(gson, type);
        if (writer != null) {
            return TypeAdapters.<T>of(Reader.of(gson.getDelegateAdapter(this, type)), writer);
        }

        return null;
    }

    private <E extends NakadiEvent> TypeAdapter<E> eventAdapter(final Gson gson, final TypeToken<E> eventType) {
        return new NakadiEventTypeAdapter<>(gson, eventType, userAdapters.get(eventType));
    }

    @SuppressWarnings("unchecked")
    private <T> Reader<? extends T> getReader(final Gson gson, final TypeToken<T> type) {
        final Class<? super T> rawType = type.getRawType();

        if (rawType == Cursor.class) {
            return (Reader<? extends T>) Reader.of(gson.getAdapter(JsonElement.class)).andThen(GsonCursor::new);
        }

        if (rawType == StreamInfo.class) {
            return (Reader<? extends T>) Reader.of(gson.getAdapter(JsonElement.class)).andThen(GsonStreamInfo::new);
        }

        return getImmutableReader(gson, type);
    }

    @SuppressWarnings("unchecked")
    private static <T> Writer<? super T> getWriter(final Gson gson, final TypeToken<T> type) {
        if (JsonElementWrapper.class.isAssignableFrom(type.getRawType())) {
            return (Writer<? super T>)
                Writer.of(gson.getAdapter(JsonElement.class)) //
                      .compose(JsonElementWrapper::wrapped)   //
                      .serializeNulls(true);
        }

        return null;
    }

    private <T> Reader<? extends T> getImmutableReader(final Gson gson, final TypeToken<T> type) {
        final Type genericType = type.getType();
        final Class<? super T> rawType = type.getRawType();

        if (!rawType.isInterface()) {
            return null;
        }

        final Class<?> implClass;
        try {
            final String implClasssName = String.format("%s.Immutable%s", //
                    rawType.getPackage().getName(), rawType.getSimpleName());
            implClass = Class.forName(implClasssName);
        } catch (@SuppressWarnings("unused") final ClassNotFoundException notFound) {
            return null;
        }

        if (!rawType.isAssignableFrom(implClass)) {
            return null;
        }

        if (rawType.equals(genericType)) {
            @SuppressWarnings("unchecked") // checked above
            final Class<? extends T> cast = (Class<? extends T>) implClass;
            return Reader.of(gson.getDelegateAdapter(this, TypeToken.get(cast)));
        }

        if (genericType instanceof ParameterizedType) {
            final Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
            @SuppressWarnings("unchecked") // Immutables will copy generic type arguments 1:1
            final TypeToken<? extends T> implToken = //
                (TypeToken<? extends T>) TypeToken.getParameterized(implClass, typeArguments);

            return Reader.of(gson.getDelegateAdapter(this, implToken));
        }

        return null;
    }
}
