package org.zalando.rxnakadi.internal;

import java.lang.reflect.Type;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import javax.inject.Inject;

import org.zalando.rxnakadi.domain.EventBatch;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.TypeRef;

/**
 * Parses raw JSON input into its respective Nakadi domain model hierarchy.
 */
final class EventBatchParser {

    private final JsonPath cursorPath = JsonPath.compile("$.cursor");
    private final JsonPath eventsPath = JsonPath.compile("$.events");

    private final ParseContext parseContext;

    @Inject
    EventBatchParser(final ParseContext parseContext) {
        this.parseContext = parseContext;
    }

    public <E> Function<String, EventBatch<E>> forType(final TypeToken<E> eventType) {

        final TypeRef<List<E>> typeRef = new TypeRef<List<E>>() {
            private final Type eventListType =     //
                new TypeToken<List<E>>() { }.where(new TypeParameter<E>() { }, eventType).getType();

            @Override
            public Type getType() {
                return eventListType;
            }
        };

        return payload -> parse(typeRef, payload);
    }

    private <E> EventBatch<E> parse(final TypeRef<List<E>> typeRef, final String payload) {
        final DocumentContext context = parseContext.parse(payload);
        return new EventBatch<>(context.read(cursorPath), getEvents(typeRef, context));
    }

    private <E> List<E> getEvents(final TypeRef<List<E>> typeRef, final DocumentContext context) {
        try {
            return ImmutableList.copyOf(context.read(eventsPath, typeRef));
        } catch (@SuppressWarnings("unused") final PathNotFoundException noEventsFound) {
            return Collections.emptyList();
        }
    }
}
