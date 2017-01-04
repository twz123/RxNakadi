package org.zalando.rxnakadi.internal;

import static java.util.Objects.requireNonNull;

import javax.inject.Inject;

import org.zalando.rxnakadi.NakadiTopic;
import org.zalando.rxnakadi.NakadiTopicFactory;
import org.zalando.rxnakadi.TopicDescriptor;
import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.http.NakadiHttpClient;

public final class DefaultNakadiTopicFactory implements NakadiTopicFactory {

    private final NakadiHttpClient http;
    private final JsonCoder json;

    @Inject
    DefaultNakadiTopicFactory(final NakadiHttpClient http, final JsonCoder json) {
        this.http = requireNonNull(http);
        this.json = requireNonNull(json);
    }

    @Override
    public <E extends NakadiEvent> NakadiTopic<E> create(final TopicDescriptor<E> descriptor) {
        return new DefaultNakadiTopic<>(descriptor, http, json);
    }
}
