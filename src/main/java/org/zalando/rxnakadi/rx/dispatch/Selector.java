package org.zalando.rxnakadi.rx.dispatch;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;

import io.reactivex.Single;

public interface Selector<A> extends Predicate<A> {

    default <T> Route<T, A, T> pass() {
        return flatMap(Single::just);
    }

    default <T, R> Route<T, A, R> map(final Function<? super T, ? extends R> fn) {
        return flatMap(requireNonNull(fn).andThen(Single::just));
    }

    default <T, R> Route<T, A, R> error(final Function<? super T, ? extends Throwable> fn) {
        return flatMap(fn.andThen(Single::error));
    }

    default <T, R> Route<T, A, R> flatMap(final Function<? super T, ? extends Single<? extends R>> fn) {
        requireNonNull(fn);
        return new Route<T, A, R>() {
            @Override
            public Selector<A> selector() {
                return Selector.this;
            }

            @Override
            public Single<? extends R> apply(final T value) {
                return fn.apply(value);
            }
        };
    }

    default <T, B, R> Route<T, A, R> dispatch(final Dispatcher<T, B> dispatcher,
            final List<Route<T, ? super B, R>> routes) {
        return new Route<T, A, R>() {
            @Override
            public Selector<A> selector() {
                return Selector.this;
            }

            @Override
            public Single<? extends R> apply(final T value) {
                return dispatcher.dispatch(value, routes);
            }
        };
    }

    default <T, B, R> Route<T, A, R> dispatch(final Dispatcher<T, B> dispatcher, final Route<T, ? super B, R> route) {
        return dispatch(dispatcher, Collections.singletonList(route));
    }

    default <T, B, R> Route<T, A, R> dispatch(final Dispatcher<T, B> dispatcher, //
            final Route<T, ? super B, R> first,                                  //
            final Route<T, ? super B, R> second) {
        return dispatch(dispatcher, ImmutableList.of(first, second));
    }

    default <T, B, R> Route<T, A, R> dispatch(final Dispatcher<T, B> dispatcher, //
            final Route<T, ? super B, R> first,                                  //
            final Route<T, ? super B, R> second,                                 //
            final Route<T, ? super B, R> third) {
        return dispatch(dispatcher, ImmutableList.of(first, second, third));
    }

    default <T, B, R> Route<T, A, R> dispatch(final Dispatcher<T, B> dispatcher, //
            final Route<T, ? super B, R> first,                                  //
            final Route<T, ? super B, R> second,                                 //
            final Route<T, ? super B, R> third,                                  //
            final Route<T, ? super B, R> fourth) {
        return dispatch(dispatcher, ImmutableList.of(first, second, third, fourth));
    }

    default <T, B, R> Route<T, A, R> dispatch(final Dispatcher<T, B> dispatcher, //
            final Route<T, ? super B, R> first,                                  //
            final Route<T, ? super B, R> second,                                 //
            final Route<T, ? super B, R> third,                                  //
            final Route<T, ? super B, R> fourth,                                 //
            final Route<T, ? super B, R> fifth) {
        return dispatch(dispatcher, ImmutableList.of(first, second, third, fourth, fifth));
    }
}
