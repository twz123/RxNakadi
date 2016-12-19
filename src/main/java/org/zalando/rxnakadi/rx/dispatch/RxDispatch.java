package org.zalando.rxnakadi.rx.dispatch;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import rx.Single;

import rx.functions.Func1;

public final class RxDispatch {

    @SafeVarargs
    public static <T, A, R> Func1<? super T, ? extends Single<? extends R>> dispatch( //
            final Dispatcher<T, A> dispatcher, final Route<T, ? super A, R>... routes) {
        return dispatch(dispatcher, Arrays.asList(routes));
    }

    public static <T, A, R> Func1<? super T, ? extends Single<? extends R>> dispatch( //
            final Dispatcher<T, A> dispatcher, final List<Route<T, ? super A, R>> routes) {
        requireNonNull(dispatcher);
        checkArgument(!routes.isEmpty(), "routes may not be empty");
        return value -> dispatcher.dispatch(value, routes);
    }

    public static <A> Selector<A> on(final Selector<A> selector) {
        return requireNonNull(selector);
    }

    public static <A> IterableSelector<A> on(final A attribute) {
        return new IterableSelector.SingleAttribute<A>(attribute) {
            @Override
            public String toString() {
                return "on(" + attribute + ')';
            }
        };
    }

    @SafeVarargs
    public static <A> IterableSelector<A> onAnyOf(final A... attributes) {
        return onAnyOf(ImmutableSet.copyOf(attributes));
    }

    public static <A> IterableSelector<A> onAnyOf(final Set<? extends A> attributes) {
        return new IterableSelector.AttributeSet<A>(attributes) {
            @Override
            public String toString() {
                final StringJoiner joiner = new StringJoiner(", ", "onAnyOf(", ")");
                attributes.forEach(a -> joiner.add(Objects.toString(a, null)));
                return joiner.toString();
            }
        };
    }

    public static IterableSelector<Integer> between(final int lower, final int upper) {
        checkArgument(lower <= upper, "%s > %s", lower, upper);

        final Set<Integer> ints = ContiguousSet.create(Range.closed(lower, upper), DiscreteDomain.integers());
        return new IterableSelector.AttributeSet<Integer>(ints) {
            @Override
            public String toString() {
                return "between(" + lower + ", " + upper + ')';
            }
        };
    }

    public static IterableSelector<Long> between(final long lower, final long upper) {
        checkArgument(lower <= upper, "%s > %s", lower, upper);

        final Set<Long> longs = ContiguousSet.create(Range.closed(lower, upper), DiscreteDomain.longs());
        return new IterableSelector.AttributeSet<Long>(longs) {
            @Override
            public String toString() {
                return "between(" + lower + ", " + upper + ')';
            }
        };
    }

    public static <A extends Comparable<? super A>> Selector<A> onRange(final Range<A> range) {
        checkArgument(!range.isEmpty(), "range may not be empty: %s", range);

        return new Selector<A>() {
            @Override
            public String toString() {
                return "onRange(" + range + ')';
            }

            @Override
            public boolean test(final A candidate) {
                return range.contains(candidate);
            }
        };
    }

    public static <A extends Comparable<? super A>> Selector<A> onRanges(final RangeSet<A> ranges) {
        checkArgument(!ranges.isEmpty() && !ranges.span().isEmpty(), "range may not be empty: %s", ranges);
        return new Selector<A>() {
            @Override
            public String toString() {
                return "onRanges(" + ranges + ')';
            }

            @Override
            public boolean test(final A candidate) {
                return ranges.contains(candidate);
            }
        };
    }

    private RxDispatch() {
        throw new AssertionError("No instances for you!");
    }
}
