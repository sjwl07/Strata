package io.strata.bench;

import java.util.Arrays;

/** Entry point: {@code ./gradlew :bench:run --args="<experiment> --opt=value ..."}. */
public final class Experiments {
  private Experiments() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("usage: Experiments phase1|q1|q2|q3 [--opt=value ...]");
      System.exit(2);
    }
    Args a = new Args(args, 1);
    switch (args[0]) {
      case "phase1" -> Phase1Baseline.run(a);
      case "q2" -> Q2MergeCorrectness.run(a);
      default -> throw new IllegalArgumentException("unknown experiment " + Arrays.toString(args));
    }
    System.exit(0);
  }
}
