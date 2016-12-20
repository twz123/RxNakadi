package org.zalando.rxnakadi.rx.dispatch;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Iterators;

public interface IterableSelector<A> extends Selector<A>, Iterable<A> {

    public class SingleAttribute<A> implements IterableSelector<A> {
        private final A attribute;

        public SingleAttribute(final A attribute) {
            this.attribute = attribute;
        }

        @Override
        public boolean test(final A candidate) {
            return Objects.equals(attribute, candidate);
        }

        @Override
        public Iterator<A> iterator() {
            return Iterators.singletonIterator(attribute);
        }
    }

    public class AttributeSet<A> implements IterableSelector<A> {
        private final Set<? extends A> attributes;

        public AttributeSet(final Set<? extends A> attributes) {
            checkArgument(!attributes.isEmpty(), "attributes may not be empty");
            this.attributes = attributes;
        }

        @Override
        public boolean test(final A candidate) {
            return attributes.contains(candidate);
        }

        @Override
        @SuppressWarnings("unchecked") // safe, since A is only used for return types
        public Iterator<A> iterator() {
            return (Iterator<A>) attributes.iterator();
        }
    }
}
