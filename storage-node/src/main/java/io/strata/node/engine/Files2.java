package io.strata.node.engine;

import java.io.IOException;
import java.nio.file.Path;

/** File helpers shared by the node. */
public final class Files2 {
  private Files2() {}

  public static void deleteRecursively(Path p) throws IOException {
    PartitionVersion.deleteRecursively(p);
  }
}
