package org.zalando.rxnakadi.http.ahc;

import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Response;

import org.asynchttpclient.netty.NettyResponse;

import rx.Observable;
import rx.Subscriber;

final class StreamedResponses {

    public static Observable.Operator<HttpResponseBodyPart, HttpResponseBodyPart> takeBytes(final long limit) {
        return child -> {
            return new Subscriber<HttpResponseBodyPart>(child) {
                private long bytes;
                private boolean completed;

                @Override
                public void onCompleted() {
                    if (!completed) {
                        completed = true;
                        child.onCompleted();
                    }
                }

                @Override
                public void onError(final Throwable e) {
                    if (!completed) {
                        completed = true;
                        child.onError(e);
                    }
                }

                @Override
                public void onNext(final HttpResponseBodyPart bodyPart) {
                    if (!isUnsubscribed()) {
                        child.onNext(bodyPart);
                        if ((bytes += bodyPart.length()) > limit && !completed) {
                            completed = true;
                            try {
                                child.onCompleted();
                            } finally {
                                unsubscribe();
                            }
                        }
                    }
                }
            };
        };
    }

    public static Observable.Transformer<HttpResponseBodyPart, Response> toResponse(final HttpResponseStatus status,
            final HttpResponseHeaders headers) {
        return partObservable -> partObservable.toList().map(parts -> new NettyResponse(status, headers, parts));
    }

    private StreamedResponses() {
        throw new AssertionError("No instances for you!");
    }
}
