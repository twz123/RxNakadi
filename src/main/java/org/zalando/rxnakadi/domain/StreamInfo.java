package org.zalando.rxnakadi.domain;

/**
 * Contains general information about a Nakadi event stream. Used only for debugging purposes. We recommend logging this
 * object in order to solve connection issues. Clients should not parse this structure.
 *
 * @see  <a href="https://github.com/zalando/nakadi/blob/R2017_01_03/api/nakadi-event-bus-api.yaml#L1359">Nakadi Event
 *       Bus API Definition: #/definitions/StreamInfo</a>
 */
public interface StreamInfo {

    /**
     * @return  debug information represented by this {@code StreamInfo}
     */
    @Override
    String toString();

}
