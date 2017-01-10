package org.zalando.rxnakadi.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import java.io.IOException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Matcher;

import org.junit.Rule;
import org.junit.Test;

import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;

import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;

import org.zalando.rxnakadi.domain.EventBatch;
import org.zalando.rxnakadi.domain.Metadata;
import org.zalando.rxnakadi.domain.NakadiEvent;

import com.google.common.base.CharMatcher;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import com.google.common.reflect.TypeToken;

@RunWith(Theories.class)
public abstract class JsonCoderTest {

    public static class SimpleEvent implements NakadiEvent {
        protected Metadata metadata;
        protected Object data;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("metadata", metadata).add("data", data).toString();
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public NakadiEvent withMetadata(final Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Object getData() {
            return data;
        }
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    protected abstract JsonCoder underTest();

    @DataPoints("roundtripProperties")
    public static List<String> roundtripProperties() {
        return ImmutableList.of("cursor", "info");
    }

    @DataPoints("roundtripValues")
    public static Collection<Map.Entry<String, Matcher<Object>>> roundtripValues() {
        return ImmutableMultimap.of(                  //
                                    "{}", is(nullValue()), //
                                    "{\"%s\":null}", hasToString("null"), //
                                    "{\"%s\":42}", hasToString("42") //
            ).entries();
    }

    @Test
    public void readsOffsetInstants() {
        assertThat(underTest().fromJson("\"2016-03-15T23:47:02.123+01:00\"", TypeToken.of(Instant.class)),
            is(LocalDateTime.of(2016, 3, 15, 23, 47, 2, (int) TimeUnit.MILLISECONDS.toNanos(123)).toInstant(
                    ZoneOffset.ofHours(1))));
    }

    @Theory
    public void readsAndWritesRoundtripProperty(                                   //
            @FromDataPoints("roundtripProperties") final String roundtripProperty, //
            @FromDataPoints("roundtripValues") final Map.Entry<String, Matcher<Object>> value) {

        final JsonCoder underTest = underTest();
        final String json = String.format(value.getKey(), roundtripProperty);
        final EventBatch<?> parsed = underTest.fromJson(json, TypeToken.of(EventBatch.class));
        assertThat(parsed, hasProperty(roundtripProperty, value.getValue()));
        assertThat(underTest.toJson(parsed), is(json));
    }

    @Test
    public void readsNullEvent() {
        assertThat(underTest().fromJson("null", TypeToken.of(SimpleEvent.class)), is(nullValue()));
    }

    public static final class EventContainer {
        final List<SimpleEvent> events;

        EventContainer(final List<SimpleEvent> events) {
            this.events = events;
        }
    }

    @Test
    public void writesNullEvent() {
        assertThat(underTest().toJson(new EventContainer(Arrays.asList((SimpleEvent) null))),
            is("{\"events\":[null]}"));
    }

    @Test
    public void writesEmptyEvent() {
        assertThat(underTest().toJson(new SimpleEvent()), is("{}"));
    }

    @Test
    public void niceErrorMessageIfEventIsNotAnObject() {
        expected.expectMessage(startsWith("Expected a JSON object"));
        underTest().fromJson("[]", TypeToken.of(SimpleEvent.class));
    }

    @Test
    public void readsAndWritesBasicEventBatch() {
        final JsonCoder underTest = underTest();
        final TypeToken<EventBatch<SimpleEvent>> ebt = new TypeToken<EventBatch<SimpleEvent>>() { };

        final String json = loadResource("basic-event-batch.json");
        final EventBatch<SimpleEvent> parsed = underTest.fromJson(json, ebt);

        assertThat(parsed.getCursor(), allOf(      //
                hasProperty("partition", is("1")), //
                hasProperty("offset", is("2"))));
        assertThat(parsed.getEvents(), hasSize(3));
        assertThat(parsed.getEvents().get(0),
            isSimpleEvent(UUID.fromString("d765de34-09c0-4bbb-8b1e-7160a33a0791"),
                LocalDateTime.of(2016, 3, 15, 22, 47, 1).toInstant(ZoneOffset.UTC), "first"));
        assertThat(parsed.getEvents().get(1),
            isSimpleEvent(UUID.fromString("a7671c51-49d1-48e6-bb03-b50dcf14f3d3"),
                LocalDateTime.of(2016, 3, 15, 22, 47, 2).toInstant(ZoneOffset.UTC), "second"));
        assertThat(parsed.getEvents().get(2),
            isSimpleEvent(UUID.fromString("6cd8f6ba-8858-4129-a32f-5fcd7a341bba"),
                LocalDateTime.of(2016, 3, 15, 22, 47, 3).toInstant(ZoneOffset.UTC), "third"));

        assertThat(underTest.toJson(parsed), is(CharMatcher.whitespace().removeFrom(json)));
    }

    private static Matcher<SimpleEvent> isSimpleEvent(final UUID eid, final Instant occurredAt, final Object data) {
        return allOf(                                                    //
                instanceOf(SimpleEvent.class),                           //
                allOf(                                                   //
                    hasProperty("metadata",
                        allOf(                                           //
                            hasProperty("eid", is(eid)),                 //
                            hasProperty("occurredAt", is(occurredAt)))), //
                    hasProperty("data", is(data))));
    }

    private static String loadResource(final String name) {
        try {
            return Resources.toString(Resources.getResource(name), UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
