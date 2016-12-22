package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class AutoCommit {

    private final long delay;
    private final TimeUnit unit;

    private AutoCommit(final long delay, final TimeUnit unit) {
        checkArgument(delay > 0, "delay must be positive: %s", delay);
        this.delay = delay;
        this.unit = requireNonNull(unit);
    }

    public static AutoCommit every(final long delay, final TimeUnit unit) {
        return new AutoCommit(delay, unit) {
            @Override
            public String toString() {
                return String.format("AutoCommit.every(%s %s)", delay, unit);
            }
        };
    }

    public <R> R apply(final BiFunction<Long, TimeUnit, R> fn) {
        return fn.apply(delay, unit);
    }

    public void accept(final BiConsumer<Long, TimeUnit> consumer) {
        consumer.accept(delay, unit);
    }
}
