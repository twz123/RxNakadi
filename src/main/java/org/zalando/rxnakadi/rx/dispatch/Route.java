package org.zalando.rxnakadi.rx.dispatch;

import rx.Single;

public interface Route<T, A, R> {

    Selector<A> selector();

    Single<? extends R> apply(T value);

}
