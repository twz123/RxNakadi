package org.zalando.rxnakadi.internal;

import java.util.List;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * Additional utility methods when working with Guava's {@link TypeToken TypeTokens}.
 */
@SuppressWarnings("serial") // for anonymous Type* instances
public final class TypeTokens {

    public static <T> TypeToken<List<T>> listOf(final Class<T> elementClass) {
        return listOf(TypeToken.of(elementClass));
    }

    public static <T> TypeToken<List<T>> listOf(final TypeToken<T> elementType) {
        return new TypeToken<List<T>>() { }.where(new TypeParameter<T>() { }, elementType);
    }

    private TypeTokens() {
        throw new AssertionError("No instances for you!");
    }
}
