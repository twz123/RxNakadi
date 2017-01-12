package org.zalando.rxnakadi.domain;

import java.util.List;

import javax.annotation.Nullable;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.immutables.value.Value;

import org.zalando.rxnakadi.StreamParameters;

/**
 * One chunk of events in a Nakadi event stream. A batch consists of a list of {@link #getEvents() events} plus a
 * {@link cursor} pointing to the offset of the last event in the batch.
 *
 * <p>The number of events in a batch is limited by the {@link StreamParameters parameters} used to initialize a
 * stream.</p>
 *
 * <p>If acting as a keep alive message (i.e. no events have arrived during the
 * {@link StreamParameters#batchFlushTimeout() batch flush timeout}), the batch won't contain any events. Sequential
 * batches might present repeated cursors if no new events have arrived.</p>
 *
 * @param  <E>  type of events in this batch
 *
 * @see    <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1414">Nakadi Event
 *         Bus API Definition: #/definitions/EventStreamBatch</a>
 */
@Value.Immutable
public interface EventBatch<E> {

    @NotNull
    @Valid
    Cursor getCursor();

    /**
     * Returns the events contained in this event batch. Potentially {@code null} or {@link List#isEmpty() empty}.
     */
    @Nullable
    List<E> getEvents();

    @Nullable
    StreamInfo getInfo();

}
