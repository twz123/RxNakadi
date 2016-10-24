package org.zalando.undertaking.oauth2;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;

import com.google.common.hash.Hashing;

public final class AccessToken {

    private final String value;

    private AccessToken(final String value) {
        this.value = requireNonNull(value);
    }

    public static AccessToken of(final String value) {
        return new AccessToken(value);
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "AccessToken(" + Hashing.sha1().hashString(value, StandardCharsets.UTF_8) + ')';
    }

    @Override
    public int hashCode() {
        return 31 + value.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj == this || (obj instanceof AccessToken && value.equals(((AccessToken) obj).value));
    }
}
