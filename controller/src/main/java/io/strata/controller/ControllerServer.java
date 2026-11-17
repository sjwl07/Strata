package io.strata.controller;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/** A running controller: state plus gRPC server. */
public final class ControllerServer implements AutoCloseable {

  private final ControllerState state;
  private final Server server;

  public ControllerServer(String host, int port, long nodeTimeoutMs) throws IOException {
    this.state = new ControllerState(System::currentTimeMillis, nodeTimeoutMs);
    this.server =
        NettyServerBuilder.forAddress(new InetSocketAddress(host, port))
            .addService(new ControllerService(state))
            .build()
            .start();
  }

  public ControllerState state() {
    return state;
  }

  public String address() {
    return "127.0.0.1:" + server.getPort();
  }

  public int port() {
    return server.getPort();
  }

  @Override
  public void close() {
    server.shutdownNow();
    try {
      server.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
