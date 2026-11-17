package io.strata.bench;

import java.util.HashMap;
import java.util.Map;

/** {@code --name=value} command-line options with defaults. */
public final class Args {
  private final Map<String, String> values = new HashMap<>();

  public Args(String[] args, int from) {
    for (int i = from; i < args.length; i++) {
      String a = args[i];
      if (!a.startsWith("--") || !a.contains("=")) {
        throw new IllegalArgumentException("expected --name=value, got " + a);
      }
      values.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
    }
  }

  public long l(String name, long def) {
    return values.containsKey(name) ? Long.parseLong(values.get(name).replace("_", "")) : def;
  }

  public int i(String name, int def) {
    return (int) l(name, def);
  }

  public double d(String name, double def) {
    return values.containsKey(name) ? Double.parseDouble(values.get(name)) : def;
  }

  public String s(String name, String def) {
    return values.getOrDefault(name, def);
  }
}
