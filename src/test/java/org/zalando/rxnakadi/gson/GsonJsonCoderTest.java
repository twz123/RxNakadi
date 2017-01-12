package org.zalando.rxnakadi.gson;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.zalando.rxnakadi.internal.JsonCoderTest;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;

public class GsonJsonCoderTest extends JsonCoderTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule().silent();

    @Mock
    private TypeAdapter<SimpleEvent> eventAdapter;

    @Mock
    private TypeAdapters.Provider userAdapters;

    @InjectMocks
    private GsonJsonCoder underTest;

    @Before
    public void initializeTest() {
        final Gson gson = new Gson();
        when(userAdapters.get(any(TypeToken.class))).then(invocation ->
                gson.getAdapter(invocation.<TypeToken>getArgument(0)));
    }

    @Override
    protected final GsonJsonCoder underTest() {
        return underTest;
    }

    @Override
    public void readsAndWritesBasicEventBatch() {
        super.readsAndWritesBasicEventBatch();
        verify(userAdapters).get(TypeToken.get(SimpleEvent.class));
    }

    @Test
    public void readsNullIfUserAdapterReturnsNullEvent() throws IOException {
        when(userAdapters.get(TypeToken.get(SimpleEvent.class))).thenReturn(eventAdapter);
        assertThat(underTest.fromJson("{}", com.google.common.reflect.TypeToken.of(SimpleEvent.class)),
            is(nullValue()));
        verify(eventAdapter).read(any());
    }

    @Test
    public void writesNothingIfUserAdapterReturnsJsonNull() throws IOException {
        when(userAdapters.get(TypeToken.get(SimpleEvent.class))).thenReturn(eventAdapter);
        doAnswer(invocation -> {
                                    invocation.<JsonWriter>getArgument(0).nullValue();
                                    return null;
                                }).when(eventAdapter).write(any(), any());

        assertThat(underTest.toJson(new SimpleEvent()), is("{}"));
        verify(eventAdapter).write(any(), any(SimpleEvent.class));
    }

    @Test
    public void niceErrorMessageIfUserAdapterReturnsNotAnObject() throws IOException {
        expected.expectMessage(startsWith("Expected a JSON object"));
        when(userAdapters.get(TypeToken.get(SimpleEvent.class))).thenReturn(eventAdapter);
        doAnswer(invocation -> {
                                    invocation.<JsonWriter>getArgument(0).value("I am a string!");
                                    return null;
                                }).when(eventAdapter).write(any(), any());

        underTest.toJson(new SimpleEvent());
    }
}
