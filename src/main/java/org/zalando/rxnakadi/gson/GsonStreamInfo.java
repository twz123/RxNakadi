package org.zalando.rxnakadi.gson;

import org.zalando.rxnakadi.domain.StreamInfo;

import com.google.gson.JsonElement;

final class GsonStreamInfo extends JsonElementWrapper implements StreamInfo {

    GsonStreamInfo(final JsonElement streamInfo) {
        super(streamInfo);
    }

}
