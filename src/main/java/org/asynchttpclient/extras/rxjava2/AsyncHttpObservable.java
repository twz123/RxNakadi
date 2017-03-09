package org.asynchttpclient.extras.rxjava2;

import java.util.function.Supplier;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import io.reactivex.Observable;

import io.reactivex.subjects.ReplaySubject;

/**
 * Provide RxJava support for executing requests. Request can be subscribed to and manipulated as needed.
 *
 * @see  <a href="https://github.com/ReactiveX/RxJava">https://github.com/ReactiveX/RxJava</a>
 */
public class AsyncHttpObservable {

    /**
     * Observe a request execution and emit the response to the observer.
     *
     * @param   supplier  the supplier
     *
     * @return  The cold observable (must be subscribed to in order to execute).
     */
    public static Observable<Response> toObservable(final Supplier<BoundRequestBuilder> supplier) {
        final BoundRequestBuilder builder = supplier.get();

        return Observable.create(emitter -> {
                try {
                    AsyncCompletionHandler<Void> handler = new AsyncCompletionHandler<Void>() {
                        @Override
                        public Void onCompleted(final Response response) throws Exception {
                            emitter.onNext(response);
                            emitter.onComplete();

                            return null;
                        }

                        @Override
                        public void onThrowable(final Throwable t) {
                            emitter.onError(t);
                        }
                    };

                    builder.execute(handler);
                } catch (Throwable t) {
                    emitter.onError(t);
                }
            });

    }

    /**
     * Observe a request execution and emit the response to the observer.
     *
     * @param   supplier  the supplier
     *
     * @return  The hot observable (eagerly executes).
     */
    public static Observable<Response> observe(final Supplier<BoundRequestBuilder> supplier) {

        // use a ReplaySubject to buffer the eagerly subscribed-to Observable
        ReplaySubject<Response> subject = ReplaySubject.create();

        // eagerly kick off subscription
        toObservable(supplier).subscribe(subject);

        // return the subject that can be subscribed to later while the execution has already started
        return subject;
    }
}
