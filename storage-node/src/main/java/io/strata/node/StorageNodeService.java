package io.strata.node;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.strata.proto.v1.BatchGetRequest;
import io.strata.proto.v1.BatchGetResponse;
import io.strata.proto.v1.GetRequest;
import io.strata.proto.v1.GetResponse;
import io.strata.proto.v1.KeyValue;
import io.strata.proto.v1.StorageNodeGrpc;
import java.util.Map;

/**
 * Phase 0 storage node: an immutable in-memory map served over gRPC.
 *
 * <p>Started from the user's draft of {@code get}. Fixes applied: set {@code found} (proto3 has no
 * "null", so a missing value and an empty value look the same without it); report bad input as
 * {@code INVALID_ARGUMENT} rather than an exception that gRPC maps to {@code UNKNOWN}; copy the map
 * so callers can't mutate it underneath concurrent readers.
 */
public class StorageNodeService extends StorageNodeGrpc.StorageNodeImplBase {

  private final Map<ByteString, ByteString> data;

  public StorageNodeService(Map<ByteString, ByteString> data) {
    // Map.copyOf is immutable, so it is safe to read from many gRPC threads without locks.
    this.data = Map.copyOf(data);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    if (request.getKey().isEmpty()) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("key must not be empty").asRuntimeException());
      return;
    }
    ByteString value = data.get(request.getKey());
    GetResponse.Builder response = GetResponse.newBuilder().setFound(value != null);
    if (value != null) {
      response.setValue(value);
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void batchGet(BatchGetRequest request, StreamObserver<BatchGetResponse> responseObserver) {
    // Validate everything first so we never send a partial answer.
    for (ByteString key : request.getKeysList()) {
      if (key.isEmpty()) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("keys must not be empty").asRuntimeException());
        return;
      }
    }
    BatchGetResponse.Builder response = BatchGetResponse.newBuilder();
    for (ByteString key : request.getKeysList()) {
      ByteString value = data.get(key);
      KeyValue.Builder entry = KeyValue.newBuilder().setKey(key).setFound(value != null);
      if (value != null) {
        entry.setValue(value);
      }
      response.addEntries(entry);
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }
}
