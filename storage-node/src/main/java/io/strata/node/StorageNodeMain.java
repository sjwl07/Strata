package io.strata.node;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Phase 0 entry point: serves a few fixed keys. Replaced by the real node in Phase 1. */
public final class StorageNodeMain {

  private StorageNodeMain() {}

  public static void main(String[] args) throws Exception {
    int port = 7070;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--port") && i + 1 < args.length) {
        port = Integer.parseInt(args[++i]);
      }
    }
    Map<ByteString, ByteString> data =
        Map.of(
            ByteString.copyFromUtf8("alice"), ByteString.copyFromUtf8("1"),
            ByteString.copyFromUtf8("bob"), ByteString.copyFromUtf8("2"));
    Server server =
        NettyServerBuilder.forPort(port).addService(new StorageNodeService(data)).build().start();
    System.out.println("storage node listening on " + port);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.shutdown();
                  try {
                    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                      server.shutdownNow();
                    }
                  } catch (InterruptedException e) {
                    server.shutdownNow();
                    Thread.currentThread().interrupt();
                  }
                }));
    server.awaitTermination();
  }
}
