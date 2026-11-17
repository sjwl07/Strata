package io.strata.push;

import java.io.IOException;
import java.util.Map;

/** An in-memory snapshot, for tests and the correctness checker. */
public record MapDataset(Map<String, byte[]> rows) implements Dataset {
  @Override
  public void forEach(Visitor visitor) throws IOException {
    for (Map.Entry<String, byte[]> e : rows.entrySet()) {
      visitor.visit(e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8), e.getValue());
    }
  }
}
