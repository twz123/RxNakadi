package org.zalando.rxnakadi;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;

/**
 * Describes a Nakadi Subscription.
 *
 * @see  <a href="https://github.com/zalando/nakadi/#subscriptions">API Overview and Usage: Subscriptions</a>
 */
public final class SubscriptionDescriptor {

    private final String owningApplication;
    private final String consumerGroup;

    public static final class Builder {
        String owningApplication;
        String consumerGroup;

        Builder() {
            // package private
        }

        /**
         * Required: Specifies the owning application of the descriptor being built.
         *
         * @param   owningApplication  name of the owning application
         *
         * @return  this builder
         *
         * @throws  NullPointerException  if {@code owningApplication} is {@code null}
         */
        public Builder owningApplication(final String owningApplication) {
            this.owningApplication = requireNonNull(owningApplication);
            return this;
        }

        /**
         * Required: Specifies the consumer group of the descriptor being built.
         *
         * @param   consumerGroup  name of the consumer group
         *
         * @return  this builder
         *
         * @throws  NullPointerException  if {@code consumerGroup} is {@code null}
         */
        public Builder consumerGroup(final String consumerGroup) {
            this.consumerGroup = requireNonNull(consumerGroup);
            return this;
        }

        /**
         * Builds the stream descriptor using the parameters provided to this builder.
         *
         * @return  the stream descriptor
         *
         * @throws  IllegalStateException  if not all of the required parameters were specified
         */
        public SubscriptionDescriptor build() {
            checkState(owningApplication != null, "No owning application provided.");
            checkState(consumerGroup != null, "No consumer group provided.");
            return new SubscriptionDescriptor(this);
        }
    }

    SubscriptionDescriptor(final Builder builder) {
        this.owningApplication = builder.owningApplication;
        this.consumerGroup = builder.consumerGroup;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)                        //
                       .add("owningApplication", owningApplication) //
                       .add("consumerGroup", consumerGroup)         //
                       .toString();
    }

    public String getOwningApplication() {
        return owningApplication;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }
}
