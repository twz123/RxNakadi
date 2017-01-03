package org.zalando.rxnakadi.gson;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.zalando.rxnakadi.domain.Metadata;
import org.zalando.rxnakadi.domain.NakadiEvent;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Reads and writes Nakadi events using Gson. Captures {@link Metadata} and hides it from user
 * serializers/deserializers.
 *
 * @param  <E>  type of the Nakadi event
 */
final class NakadiEventTypeAdapter<E extends NakadiEvent> extends TypeAdapter<E> {

    private final TypeAdapter<JsonElement> elementAdapter;
    private final TypeAdapter<Metadata> metaAdapter;
    private final TypeAdapter<E> eventAdapter;

    public NakadiEventTypeAdapter(final TypeAdapters.Provider adapterProvider, final TypeAdapter<E> eventAdapter) {
        this.elementAdapter = adapterProvider.require(JsonElement.class);
        this.metaAdapter = adapterProvider.require(Metadata.class);
        this.eventAdapter = requireNonNull(eventAdapter);
    }

    @Override
    public E read(final JsonReader in) throws IOException {
        final JsonObject eventData = elementAdapter.read(in).getAsJsonObject();
        final JsonElement metadataData = eventData.remove("metadata");
        final E event = eventAdapter.fromJsonTree(eventData);
        if (event != null) {
            event.setMetadata(metadataData == null ? null : metaAdapter.fromJsonTree(metadataData));
        }

        return event;
    }

    @Override
    public void write(final JsonWriter out, final E value) throws IOException {

        // Nasty hack to "hide" metadata from user-provided serializers.
        // Maybe it makes sense to introduce another composite type between EventBatch and actual user data that
        // carries along the metadata?
        final JsonElement eventElement;
        final Metadata metadata = value.getMetadata();
        value.setMetadata(null);
        try {
            eventElement = eventAdapter.toJsonTree(value);
        } finally {
            value.setMetadata(metadata);
        }

        if (eventElement.isJsonObject()) {
            final JsonObject eventObject = eventElement.getAsJsonObject();
            eventObject.add("metadata", metaAdapter.toJsonTree(metadata));
            elementAdapter.write(out, eventObject);
        } else {
            elementAdapter.write(out, eventElement);
        }
    }
}
