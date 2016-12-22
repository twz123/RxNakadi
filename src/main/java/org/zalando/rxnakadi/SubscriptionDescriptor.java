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

        public Builder owningApplication(final String owningApplication) {
            this.owningApplication = requireNonNull(owningApplication);
            return this;
        }

        public Builder consumerGroup(final String consumerGroup) {
            this.consumerGroup = requireNonNull(consumerGroup);
            return this;
        }

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
