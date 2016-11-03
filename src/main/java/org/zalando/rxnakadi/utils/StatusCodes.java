package org.zalando.rxnakadi.utils;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import io.netty.handler.codec.http.HttpResponseStatus;

public final class StatusCodes {
    private StatusCodes() {
        throw new UnsupportedOperationException("The class StatusCodes shall not be instantiated!");
    }

    public static final Set<Integer> SUCCESS_CODES = ImmutableSet.of(HttpResponseStatus.OK.code(),
            HttpResponseStatus.NO_CONTENT.code());
    public static final Set<Integer> ERROR_CODES = ImmutableSet.of(HttpResponseStatus.BAD_REQUEST.code(),
            HttpResponseStatus.NOT_FOUND.code(), HttpResponseStatus.UNPROCESSABLE_ENTITY.code());

}
