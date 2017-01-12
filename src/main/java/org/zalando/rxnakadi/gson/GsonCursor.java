package org.zalando.rxnakadi.gson;

import org.zalando.rxnakadi.domain.Cursor;

import com.google.gson.JsonElement;

final class GsonCursor extends JsonElementWrapper implements Cursor {

    GsonCursor(final JsonElement cursor) {
        super(cursor);
    }

    @Override
    public String getPartition() {
        return getStringOrNull("partition");
    }

    @Override
    public String getOffset() {
        return getStringOrNull("offset");
    }

}
