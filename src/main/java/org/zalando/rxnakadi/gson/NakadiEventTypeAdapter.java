package org.zalando.rxnakadi.gson;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import java.util.Map;

import org.zalando.rxnakadi.domain.Metadata;
import org.zalando.rxnakadi.domain.NakadiEvent;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Reads and writes Nakadi events using Gson. Captures {@link Metadata} and hides it from user
 * serializers/deserializers.
 *
 * @param  <E>  type of the Nakadi event
 */
final class NakadiEventTypeAdapter<E extends NakadiEvent> extends TypeAdapter<E> {

    private final Gson gson;
    private final TypeToken<E> eventType;
    private final TypeAdapter<E> eventAdapter;

    private final TypeAdapter<JsonElement> elementAdapter;
    private final TypeAdapter<Metadata> metaAdapter;

    public NakadiEventTypeAdapter(final Gson gson, final TypeToken<E> eventType, final TypeAdapter<E> eventAdapter) {
        this.gson = requireNonNull(gson);
        this.eventType = requireNonNull(eventType);
        this.eventAdapter = requireNonNull(eventAdapter);

        this.elementAdapter = gson.getAdapter(JsonElement.class);
        this.metaAdapter = gson.getAdapter(Metadata.class);
    }

    @Override
    public E read(final JsonReader in) throws IOException {
        final JsonElement eventElement = elementAdapter.read(in);
        if (eventElement.isJsonNull()) {
            return null;
        }

        if (!eventElement.isJsonObject()) {
            throw new JsonSyntaxException("Expected a JSON object, but got " + eventElement.getClass().getSimpleName()
                    + " at " + in.getPath());
        }

        final JsonElement metadataData = ((JsonObject) eventElement).remove("metadata");
        final E event = eventAdapter.fromJsonTree(eventElement);
        if (event == null) {
            return null;
        }

        return castEvent(event.withMetadata(metadataData == null ? null : metaAdapter.fromJsonTree(metadataData)));
    }

    @Override
    public void write(final JsonWriter out, final E value) throws IOException {
        if (value == null) {
            out.nullValue();
            return;
        }

        out.beginObject();

        final Metadata metadata = value.getMetadata();
        final JsonElement eventElement;

        if (metadata == null) {
            eventElement = eventAdapter.toJsonTree(value);
        } else {

            writeMetadata(out, metadata);

            // Nasty hack to "hide" metadata from user-provided serializers.
            // Maybe it makes sense to introduce another composite type between EventBatch and actual user data that
            // carries along the metadata?

            final E withoutMetadata = castEvent(value.withMetadata(null));
            try {
                eventElement = eventAdapter.toJsonTree(withoutMetadata);
            } finally {
                if (withoutMetadata == value) { // if value was updated in place, swap back in the real metadata
                    value.withMetadata(metadata);
                }
            }
        }

        if (!eventElement.isJsonNull()) {
            if (!eventElement.isJsonObject()) {
                throw new IllegalArgumentException("Expected a JSON object, but got "
                        + eventElement.getClass().getSimpleName() + " while writing " + value);
            }

            for (final Map.Entry<String, JsonElement> property : ((JsonObject) eventElement).entrySet()) {
                out.name(property.getKey());
                elementAdapter.write(out, property.getValue());
            }
        }

        out.endObject();
    }

    private void writeMetadata(final JsonWriter out, final Metadata metadata) throws IOException {
        @SuppressWarnings("unchecked")
        final TypeAdapter<Metadata> runtimeMetaAdapter = (TypeAdapter<Metadata>) gson.getAdapter(metadata.getClass());

        out.name("metadata");
        runtimeMetaAdapter.write(out, metadata);
    }

    @SuppressWarnings("unchecked")
    private E castEvent(final NakadiEvent event) {
        return (E) eventType.getRawType().cast(event);
    }
}
