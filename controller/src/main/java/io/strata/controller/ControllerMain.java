package io.strata.controller;

/** {@code controller [--port=7000] [--node-timeout-ms=3000]} */
public final class ControllerMain {
  private ControllerMain() {}

  public static void main(String[] args) throws Exception {
    int port = 7000;
    long timeout = 3000;
    for (String a : args) {
      if (a.startsWith("--port=")) {
        port = Integer.parseInt(a.substring(7));
      } else if (a.startsWith("--node-timeout-ms=")) {
        timeout = Long.parseLong(a.substring(18));
      }
    }
    ControllerServer server = new ControllerServer("0.0.0.0", port, timeout);
    System.out.println("controller listening on " + server.port());
    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    Thread.currentThread().join();
  }
}
