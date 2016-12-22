package org.zalando.rxnakadi.inject;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.inject.guice.NakadiTopicBinder;

/**
 * Type hint for Java types representing Nakadi events.
 *
 * @see  NakadiTopicBinder
 */
@Documented
@Target(METHOD)
@Retention(RUNTIME)
public @interface NakadiEventType {

    /**
     * @return  the Java type to be used to represent Nakadi events deonoted by the annotated target element.
     */
    Class<? extends NakadiEvent> value();
}
