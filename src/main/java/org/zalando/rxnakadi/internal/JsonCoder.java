package org.zalando.rxnakadi.internal;

import com.google.common.reflect.TypeToken;

/**
 * Lightweight JSON marshaling/unmarshaling abstraction.
 *
 * @see  org.zalando.rxnakadi.gson.GsonJsonCoder Implementation using Gson
 */
public interface JsonCoder {

    <T> T fromJson(String json, TypeToken<T> type);

    String toJson(Object value);

}
