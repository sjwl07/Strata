package io.strata.node;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.strata.common.ServeHook;
import io.strata.node.engine.Lookup;
import io.strata.node.engine.NotServingException;
import io.strata.proto.v1.BatchGetRequest;
import io.strata.proto.v1.BatchGetResponse;
import io.strata.proto.v1.GetRequest;
import io.strata.proto.v1.GetResponse;
import io.strata.proto.v1.KeyValue;
import io.strata.proto.v1.ServerFeedback;
import io.strata.proto.v1.StorageNodeGrpc;
import java.util.Map;
import java.util.function.IntSupplier;

/**
 * The read API. Each response carries {@link ServerFeedback} (queue depth and service time) so
 * clients can rank replicas (C3, Phase 5).
 *
 * <p>Error mapping: empty key → {@code INVALID_ARGUMENT}; partition not hosted here or not loaded
 * yet → {@code UNAVAILABLE} (the client retries another replica); client already gave up → {@code
 * CANCELLED}, without doing the read. Skipping cancelled work is what makes hedging cheap.
 */
public class StorageNodeService extends StorageNodeGrpc.StorageNodeImplBase {

  /** Resolves a key in a store. */
  @FunctionalInterface
  public interface KeyLookup {
    Lookup get(String store, byte[] key) throws NotServingException;
  }

  private final KeyLookup lookup;
  private final IntSupplier queueDepth;
  private final ServeHook hook;

  /** Phase 0 constructor: an immutable in-memory map, one unnamed store. */
  public StorageNodeService(Map<ByteString, ByteString> data) {
    this(mapLookup(Map.copyOf(data)), () -> 0, ServeHook.NONE);
  }

  public StorageNodeService(KeyLookup lookup, IntSupplier queueDepth, ServeHook hook) {
    this.lookup = lookup;
    this.queueDepth = queueDepth;
    this.hook = hook;
  }

  private static KeyLookup mapLookup(Map<ByteString, ByteString> data) {
    return (store, key) -> {
      ByteString v = data.get(ByteString.copyFrom(key));
      return v == null ? Lookup.missing(0) : new Lookup(true, v.toByteArray(), 0);
    };
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> out) {
    if (request.getKey().isEmpty()) {
      out.onError(invalid("key must not be empty"));
      return;
    }
    if (Context.current().isCancelled()) {
      out.onError(
          Status.CANCELLED.withDescription("cancelled before serving").asRuntimeException());
      return;
    }
    long start = System.nanoTime();
    try {
      hook.beforeServe(1);
      Lookup r = lookup.get(request.getStore(), request.getKey().toByteArray());
      GetResponse.Builder b = GetResponse.newBuilder().setFound(r.found()).setVersion(r.version());
      if (r.found()) {
        b.setValue(UnsafeByteOperations.unsafeWrap(r.value()));
      }
      b.setFeedback(feedback(start));
      out.onNext(b.build());
      out.onCompleted();
    } catch (NotServingException e) {
      out.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      out.onError(Status.CANCELLED.withDescription("interrupted").asRuntimeException());
    }
  }

  @Override
  public void batchGet(BatchGetRequest request, StreamObserver<BatchGetResponse> out) {
    for (ByteString key : request.getKeysList()) {
      if (key.isEmpty()) {
        out.onError(invalid("keys must not be empty"));
        return;
      }
    }
    if (Context.current().isCancelled()) {
      out.onError(
          Status.CANCELLED.withDescription("cancelled before serving").asRuntimeException());
      return;
    }
    long start = System.nanoTime();
    try {
      hook.beforeServe(request.getKeysCount());
      BatchGetResponse.Builder b = BatchGetResponse.newBuilder();
      for (ByteString key : request.getKeysList()) {
        Lookup r = lookup.get(request.getStore(), key.toByteArray());
        KeyValue.Builder kv =
            KeyValue.newBuilder().setKey(key).setFound(r.found()).setVersion(r.version());
        if (r.found()) {
          kv.setValue(UnsafeByteOperations.unsafeWrap(r.value()));
        }
        b.addEntries(kv);
      }
      b.setFeedback(feedback(start));
      out.onNext(b.build());
      out.onCompleted();
    } catch (NotServingException e) {
      out.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      out.onError(Status.CANCELLED.withDescription("interrupted").asRuntimeException());
    }
  }

  private ServerFeedback feedback(long startNanos) {
    return ServerFeedback.newBuilder()
        .setQueueDepth(queueDepth.getAsInt())
        .setServiceTimeMicros((System.nanoTime() - startNanos) / 1000)
        .build();
  }

  private static RuntimeException invalid(String msg) {
    return Status.INVALID_ARGUMENT.withDescription(msg).asRuntimeException();
  }
}
