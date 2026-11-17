package io.strata.controller;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.strata.proto.v1.ClusterViewRequest;
import io.strata.proto.v1.ControllerGrpc;
import io.strata.proto.v1.PushStatusRequest;
import io.strata.proto.v1.StartPushRequest;
import io.strata.proto.v1.StoreSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Operator commands against a running controller.
 *
 * <pre>
 * strata-admin --controller=HOST:PORT create-store --name=S --partitions=8 --rf=3 [--mode=FULL]
 *                                                  [--skew-ms=1000]
 * strata-admin --controller=HOST:PORT start-push --store=S --version=N [--wait=true]
 * strata-admin --controller=HOST:PORT status --store=S --version=N
 * strata-admin --controller=HOST:PORT view
 * </pre>
 */
public final class AdminMain {
  private AdminMain() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> o = new HashMap<>();
    String cmd = null;
    for (String a : args) {
      if (a.startsWith("--")) {
        o.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
      } else {
        cmd = a;
      }
    }
    ManagedChannel ch =
        NettyChannelBuilder.forTarget(o.getOrDefault("controller", "localhost:7000"))
            .usePlaintext()
            .build();
    ControllerGrpc.ControllerBlockingStub c =
        ControllerGrpc.newBlockingStub(ch).withDeadlineAfter(10, TimeUnit.SECONDS);
    JsonFormat.Printer json = JsonFormat.printer();
    switch (cmd == null ? "" : cmd) {
      case "create-store" -> {
        c.createStore(
            StoreSpec.newBuilder()
                .setName(o.get("name"))
                .setNumPartitions(Integer.parseInt(o.getOrDefault("partitions", "8")))
                .setReplicationFactor(Integer.parseInt(o.getOrDefault("rf", "3")))
                .setMergeMode(o.getOrDefault("mode", "FULL"))
                .setMaxClockSkewMs(Long.parseLong(o.getOrDefault("skew-ms", "1000")))
                .build());
        System.out.println("created " + o.get("name"));
      }
      case "start-push" -> {
        String store = o.get("store");
        long v = Long.parseLong(o.get("version"));
        System.out.println(
            json.print(
                c.startPush(StartPushRequest.newBuilder().setStore(store).setVersion(v).build())));
        if (Boolean.parseBoolean(o.getOrDefault("wait", "true"))) {
          ControllerGrpc.ControllerBlockingStub poll = ControllerGrpc.newBlockingStub(ch);
          long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
          String state;
          do {
            Thread.sleep(200);
            state =
                poll.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getPushStatus(
                        PushStatusRequest.newBuilder().setStore(store).setVersion(v).build())
                    .getState();
          } while (state.equals("PENDING") && System.nanoTime() < deadline);
          System.out.println("push " + store + " v" + v + ": " + state);
        }
      }
      case "status" ->
          System.out.println(
              json.print(
                  c.getPushStatus(
                      PushStatusRequest.newBuilder()
                          .setStore(o.get("store"))
                          .setVersion(Long.parseLong(o.get("version")))
                          .build())));
      case "view" ->
          System.out.println(json.print(c.getClusterView(ClusterViewRequest.getDefaultInstance())));
      default -> {
        System.err.println("commands: create-store | start-push | status | view");
        System.exit(2);
      }
    }
    ch.shutdownNow();
  }
}
