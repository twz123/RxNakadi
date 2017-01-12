package org.zalando.rxnakadi.gson;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;

import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

@RunWith(MockitoJUnitRunner.class)
public class GuavaTypeAdapterFactoryTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final GuavaTypeAdapterFactory underTest = new GuavaTypeAdapterFactory();
    private final Gson gson = new Gson();

    private final TypeToken<ImmutableCollection<?>> wildcardCollection = new TypeToken<ImmutableCollection<?>>() { };

    @Test
    public void nullReturnsNull() {
        final TypeAdapter<? extends ImmutableCollection<?>> adapter = underTest.create(gson, wildcardCollection);
        assertThat(readJson("null", adapter), is(nullValue()));
    }

    @Test
    public void emptyReturnsEmpty() {
        final TypeAdapter<? extends ImmutableCollection<?>> adapter = underTest.create(gson, wildcardCollection);
        assertThat(readJson("[]", adapter), allOf(instanceOf(ImmutableList.class), empty()));
    }

    @Test
    public void stringReturnsString() {
        final TypeAdapter<? extends ImmutableCollection<?>> adapter = underTest.create(gson, wildcardCollection);
        assertThat(readJson("[\"foo\"]", adapter), allOf(instanceOf(ImmutableList.class), contains("foo")));
    }

    @Test
    public void rawTypeWorks() {
        final ImmutableCollection<?> collection = readJson("[\"foo\"]",
                underTest.create(gson, TypeToken.get(ImmutableCollection.class)));
        assertThat(collection, allOf(instanceOf(ImmutableList.class), contains("foo")));
    }

    @Test
    public void niceErrorMessageOnNullElement() {
        expected.expectMessage(is(
                "Encountered a null array element while reading an instance of type com.google.common.collect.ImmutableCollection<?> at $[1]"));
        readJson("[null]", underTest.create(gson, wildcardCollection));
    }

    private static <T> T readJson(final String json, final TypeAdapter<T> typeAdapter) {
        try(StringReader in = new StringReader(json);
                JsonReader reader = new JsonReader(in)) {
            final T value = typeAdapter.read(reader);
            assertThat(reader.peek(), is(JsonToken.END_DOCUMENT));
            return value;
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }
}
