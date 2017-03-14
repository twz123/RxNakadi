package org.zalando.rxnakadi.rx.dispatch;

import io.reactivex.Single;

public interface Route<T, A, R> {

    Selector<A> selector();

    Single<? extends R> apply(T value);

}
