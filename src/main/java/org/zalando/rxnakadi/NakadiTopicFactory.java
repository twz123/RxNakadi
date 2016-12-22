package org.zalando.rxnakadi;

import org.zalando.rxnakadi.domain.NakadiEvent;

/**
 * Creates ready-to-use {@link NakadiTopic Nakadi topics} from {@link TopicDescriptor topic descriptors}.
 */
public interface NakadiTopicFactory {

    /**
     * Creates a Nakadi topic for the given {@code descriptor}.
     *
     * @throws  NullPointerException  if {@code descriptor} is {@code null}
     */
    <E extends NakadiEvent> NakadiTopic<E> create(TopicDescriptor<E> descriptor);
}
