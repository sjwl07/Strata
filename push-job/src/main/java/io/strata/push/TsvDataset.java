package io.strata.push;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Text input, one {@code key<TAB>value} pair per line (UTF-8). For quickstarts and demos. */
public record TsvDataset(Path path) implements Dataset {

  @Override
  public void forEach(Visitor visitor) throws IOException {
    try (BufferedReader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      String line;
      while ((line = r.readLine()) != null) {
        if (line.isBlank() || line.startsWith("#")) {
          continue;
        }
        int tab = line.indexOf('\t');
        if (tab <= 0) {
          throw new IOException("expected key<TAB>value: " + line);
        }
        visitor.visit(
            line.substring(0, tab).getBytes(StandardCharsets.UTF_8),
            line.substring(tab + 1).getBytes(StandardCharsets.UTF_8));
      }
    }
  }
}
