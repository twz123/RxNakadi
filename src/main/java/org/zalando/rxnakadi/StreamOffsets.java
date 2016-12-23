package org.zalando.rxnakadi;

import java.util.Optional;

import org.zalando.rxnakadi.domain.Partition;

import rx.Single;

/**
 * Defines at which offset the streaming for a given partition should start.
 */
@FunctionalInterface
public interface StreamOffsets {

    String BEGIN_VALUE = "BEGIN";
    String END_VALUE = "END";

    /**
     * Starts every partition at its beginning.
     */
    StreamOffsets BEGIN = new StreamOffsets() {
        @Override
        public String toString() {
            return "StreamOffsets.BEGIN";
        }

        @Override
        public Single<Optional<String>> offsetFor(final Partition partiton) {
            return Single.just(Optional.of(BEGIN_VALUE));
        }
    };

    /**
     * Returns a {@code Single} that emits the offset at which to start streaming for a given partition. An
     * {@link Optional#empty() empty} value indicates that the given {@code partition} shouldn't be streamed at all.
     *
     * @param   partition  partition for which the starting offset is to be emitted
     *
     * @return  emits the start offset for {@code partition}, or {@link Optional#empty() empty} if it shouldn't be
     *          streamed
     */
    Single<Optional<String>> offsetFor(Partition partition);
}
