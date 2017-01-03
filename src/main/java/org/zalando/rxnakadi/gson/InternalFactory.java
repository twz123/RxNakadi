package org.zalando.rxnakadi.gson;

import static java.util.Objects.requireNonNull;

import java.time.Instant;

import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.gson.TypeAdapters.Reader;
import org.zalando.rxnakadi.gson.TypeAdapters.Writer;

import com.google.gson.Gson;
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

        if (Instant.class.isAssignableFrom(rawType)) {
            return (TypeAdapter<T>) TypeAdapters.of( //
                    Reader.fromString(Instant::parse), Writer.toStringValue()).nullSafe();
        }

        if (NakadiEvent.class.isAssignableFrom(rawType)) {
            return (TypeAdapter<T>) eventAdapter(gson, (TypeToken<? extends NakadiEvent>) type).nullSafe();
        }

        return null;
    }

    private <E extends NakadiEvent> TypeAdapter<E> eventAdapter(final Gson gson, final TypeToken<E> eventType) {
        return new NakadiEventTypeAdapter<>(TypeAdapters.Provider.of(gson), userAdapters.require(eventType));
    }
}
