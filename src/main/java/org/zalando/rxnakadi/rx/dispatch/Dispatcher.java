package org.zalando.rxnakadi.rx.dispatch;

import java.util.Arrays;
import java.util.List;

import io.reactivex.Single;

public abstract class Dispatcher<T, A> {

    public abstract <R> Single<? extends R> dispatch(final T value, final List<Route<T, ? super A, R>> routes);

    @SafeVarargs
    public final <R> Single<? extends R> dispatch(final T value, final Route<T, ? super A, R>... routes) {
        return dispatch(value, Arrays.asList(routes));
    }
}
