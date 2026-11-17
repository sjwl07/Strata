package io.strata.bench;

import io.strata.client.StrataClient;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** {@code bench get --controller=HOST:PORT --store=S --key=K}: one read through the client. */
final class GetCommand {
  private GetCommand() {}

  static void run(Args a) throws Exception {
    try (StrataClient c =
        new StrataClient(StrataClient.Options.defaults(a.s("controller", "localhost:7000")))) {
      Optional<byte[]> v =
          c.get(a.s("store", "demo"), a.s("key", "").getBytes(StandardCharsets.UTF_8)).get();
      System.out.println(v.map(b -> new String(b, StandardCharsets.UTF_8)).orElse("<not found>"));
    }
  }
}
