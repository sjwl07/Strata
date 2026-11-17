package io.strata.bench;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.CompletableFuture;

/** Small gRPC helpers for experiments. */
final class Grpc {
  private Grpc() {}

  static <T> CompletableFuture<T> toCf(ListenableFuture<T> lf) {
    CompletableFuture<T> cf = new CompletableFuture<>();
    Futures.addCallback(
        lf,
        new FutureCallback<>() {
          @Override
          public void onSuccess(T result) {
            cf.complete(result);
          }

          @Override
          public void onFailure(Throwable t) {
            cf.completeExceptionally(t);
          }
        },
        MoreExecutors.directExecutor());
    return cf;
  }
}
