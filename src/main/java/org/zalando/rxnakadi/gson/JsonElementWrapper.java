package org.zalando.rxnakadi.gson;

import static java.util.Objects.requireNonNull;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

abstract class JsonElementWrapper {

    private final JsonElement wrapped;

    JsonElementWrapper(final JsonElement wrapped) {
        this.wrapped = requireNonNull(wrapped);
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }

    final JsonElement wrapped() {
        return wrapped;
    }

    final String getStringOrNull(final String name) {
        if (wrapped instanceof JsonObject) {
            final JsonElement element = ((JsonObject) wrapped).get(name);
            return element instanceof JsonPrimitive ? ((JsonPrimitive) element).getAsString() : null;
        }

        return null;
    }
}
