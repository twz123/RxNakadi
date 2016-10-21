package org.zalando.rxnakadi.http.resource;

import static com.google.common.base.Preconditions.checkArgument;

public class Problem {

    private long timestamp;

    private String type;
    private String title;
    private int status;
    private String detail;
    private String instance;

    public Problem(final Builder builder) {
        this.type = builder.type;
        this.title = builder.title;
        this.status = builder.statusCode;
        this.detail = builder.detail;
        this.instance = builder.instance;
        this.timestamp = builder.timestamp == Long.MIN_VALUE ? System.currentTimeMillis() : builder.timestamp;
    }

    public static class Builder {

        final int statusCode;

        String type = "about:blank";
        String title;
        String detail;
        String instance;
        long timestamp = Long.MIN_VALUE;

        private Builder(final int statusCode) {
            checkArgument(statusCode >= 100 && statusCode <= 599, "status out of range");
            this.statusCode = statusCode;
        }

        public static Builder forStatus(final int status) {
            final int statusCode = status;
            final Builder builder = new Builder(statusCode);
            builder.type = "https://httpstatuses.com/" + statusCode;

            return builder;
        }

        public Builder withType(final String type) {
            this.type = type;
            return this;
        }

        public Builder withTitle(final String title) {
            this.title = title;
            return this;
        }

        public Builder withDetail(final String detail) {
            this.detail = detail;

            return this;
        }

        public Builder withInstance(final String instance) {
            this.instance = instance;
            return this;
        }

        public Problem build() {
            return new Problem(this);
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getType() {
        return type;
    }

    public String getTitle() {
        return title;
    }

    public int getStatus() {
        return status;
    }

    public String getDetail() {
        return detail;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(final String instance) {
        this.instance = instance;
    }
}
