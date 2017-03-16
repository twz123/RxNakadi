package org.asynchttpclient.extras.rxjava2.single;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import org.asynchttpclient.handler.ProgressAsyncHandler;

import io.reactivex.Single;
import io.reactivex.SingleEmitter;

import io.reactivex.disposables.Disposables;

/**
 * Wraps HTTP requests into RxJava {@code Single} instances.
 *
 * @see  <a href="https://github.com/ReactiveX/RxJava">https://github.com/ ReactiveX/RxJava</a>
 */
public final class AsyncHttpSingle {

    private AsyncHttpSingle() {
        throw new AssertionError("No instances for you!");
    }

    /**
     * Emits the responses to HTTP requests obtained from {@code builder}.
     *
     * @param   builder  used to build the HTTP request that is to be executed
     *
     * @return  a {@code Single} that executes new requests on subscription obtained from {@code builder} on
     *          subscription and that emits the response
     *
     * @throws  NullPointerException  if {@code builder} is {@code null}
     */
    public static Single<Response> create(final BoundRequestBuilder builder) {
        requireNonNull(builder);
        return create(builder::execute, AsyncCompletionHandlerBase::new);
    }

    /**
     * Emits the responses to HTTP requests obtained by calling {@code requestTemplate}.
     *
     * @param   requestTemplate  called to start the HTTP request with an {@code AysncHandler} that builds the HTTP
     *                           response and propagates results to the returned {@code Single}. The {@code Future} that
     *                           is returned by {@code requestTemplate} will be used to cancel the request when the
     *                           {@code Single} is unsubscribed.
     *
     * @return  a {@code Single} that executes new requests on subscription by calling {@code requestTemplate} and that
     *          emits the response
     *
     * @throws  NullPointerException  if {@code requestTemplate} is {@code null}
     */
    public static Single<Response> create(
            final Function<? super AsyncHandler<?>, ? extends Future<?>> requestTemplate) {
        return create(requestTemplate, AsyncCompletionHandlerBase::new);
    }

    /**
     * Emits the results of {@code AsyncHandlers} obtained from {@code handlerSupplier} for HTTP requests obtained from
     * {@code builder}.
     *
     * @param   builder          used to build the HTTP request that is to be executed
     * @param   handlerSupplier  supplies the desired {@code AsyncHandler} instances that are used to produce results
     *
     * @return  a {@code Single} that executes new requests on subscription obtained from {@code builder} and that emits
     *          the result of the {@code AsyncHandler} obtained from {@code handlerSupplier}
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    public static <T> Single<T> create(final BoundRequestBuilder builder,
            final Supplier<? extends AsyncHandler<? extends T>> handlerSupplier) {

        requireNonNull(builder);
        return create(builder::execute, handlerSupplier);
    }

    /**
     * Emits the results of {@code AsyncHandlers} obtained from {@code handlerSupplier} for HTTP requests obtained
     * obtained by calling {@code requestTemplate}.
     *
     * @param   requestTemplate  called to start the HTTP request with an {@code AysncHandler} that builds the HTTP
     *                           response and propagates results to the returned {@code Single}. The {@code Future} that
     *                           is returned by {@code requestTemplate} will be used to cancel the request when the
     *                           {@code Single} is unsubscribed.
     * @param   handlerSupplier  supplies the desired {@code AsyncHandler} instances that are used to produce results
     *
     * @return  a {@code Single} that executes new requests on subscription by calling {@code requestTemplate} and that
     *          emits the results produced by the {@code AsyncHandlers} supplied by {@code handlerSupplier}
     *
     * @throws  NullPointerException  if at least one of the parameters is {@code null}
     */
    public static <T> Single<T> create(final Function<? super AsyncHandler<?>, ? extends Future<?>> requestTemplate,
            final Supplier<? extends AsyncHandler<? extends T>> handlerSupplier) {

        requireNonNull(requestTemplate);
        requireNonNull(handlerSupplier);

        return Single.create(subscriber -> {
                final AsyncHandler<?> bridge = createBridge(subscriber, handlerSupplier.get());
                final Future<?> responseFuture = requestTemplate.apply(bridge);
                subscriber.setDisposable(Disposables.fromFuture(responseFuture));
            });
    }

    static <T> AsyncHandler<?> createBridge(final SingleEmitter<? super T> subscriber,
            final AsyncHandler<? extends T> handler) {
        if (handler instanceof ProgressAsyncHandler) {
            return new ProgressAsyncSingleEmitterBridge<>(subscriber, (ProgressAsyncHandler<? extends T>) handler);
        }

        return new AsyncSingleEmitterBridge<>(subscriber, handler);
    }
}
