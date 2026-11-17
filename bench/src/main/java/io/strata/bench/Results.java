package io.strata.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Writes experiment rows as CSV under the output directory. */
final class Results {
  private Results() {}

  static void csv(Path dir, String name, String header, List<String> rows) throws IOException {
    Files.createDirectories(dir);
    Path f = dir.resolve(name);
    StringBuilder sb = new StringBuilder(header).append('\n');
    rows.forEach(r -> sb.append(r).append('\n'));
    Files.writeString(f, sb);
    System.out.println("wrote " + f);
  }
}
