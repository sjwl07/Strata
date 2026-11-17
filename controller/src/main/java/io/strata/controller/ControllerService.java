package io.strata.controller;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.strata.proto.v1.ClusterView;
import io.strata.proto.v1.ClusterViewRequest;
import io.strata.proto.v1.ControllerGrpc;
import io.strata.proto.v1.CreateStoreResponse;
import io.strata.proto.v1.HeartbeatRequest;
import io.strata.proto.v1.HeartbeatResponse;
import io.strata.proto.v1.PushStatus;
import io.strata.proto.v1.PushStatusRequest;
import io.strata.proto.v1.StartPushRequest;
import io.strata.proto.v1.StoreSpec;
import java.util.function.Supplier;

/** gRPC front end for {@link ControllerState}. */
public final class ControllerService extends ControllerGrpc.ControllerImplBase {

  private final ControllerState state;

  public ControllerService(ControllerState state) {
    this.state = state;
  }

  @Override
  public void heartbeat(HeartbeatRequest req, StreamObserver<HeartbeatResponse> out) {
    reply(out, () -> state.heartbeat(req));
  }

  @Override
  public void getClusterView(ClusterViewRequest req, StreamObserver<ClusterView> out) {
    reply(out, state::view);
  }

  @Override
  public void createStore(StoreSpec spec, StreamObserver<CreateStoreResponse> out) {
    reply(
        out,
        () -> {
          state.createStore(spec);
          return CreateStoreResponse.getDefaultInstance();
        });
  }

  @Override
  public void startPush(StartPushRequest req, StreamObserver<PushStatus> out) {
    reply(out, () -> state.startPush(req.getStore(), req.getVersion()));
  }

  @Override
  public void getPushStatus(PushStatusRequest req, StreamObserver<PushStatus> out) {
    reply(out, () -> state.pushStatus(req.getStore(), req.getVersion()));
  }

  private static <T> void reply(StreamObserver<T> out, Supplier<T> body) {
    T result;
    try {
      result = body.get();
    } catch (IllegalArgumentException e) {
      out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
      return;
    }
    out.onNext(result);
    out.onCompleted();
  }
}
