package io.strata.checker;

import io.strata.checker.Checker.Anomaly;
import io.strata.checker.Checker.Report;
import io.strata.node.engine.MergeMode;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Runs many random histories per mode and prints an anomaly table.
 *
 * <pre>./gradlew :checker:run --args="--histories=200 --seed=1 --modes=FULL,LWW,NAIVE"</pre>
 *
 * Exit code 1 if FULL mode (with a correct skew bound) shows any anomaly.
 */
public final class CheckerMain {
  private CheckerMain() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> opts = new LinkedHashMap<>();
    for (String a : args) {
      opts.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
    }
    int histories = Integer.parseInt(opts.getOrDefault("histories", "100"));
    long seed = Long.parseLong(opts.getOrDefault("seed", "1"));
    String[] modes = opts.getOrDefault("modes", "FULL,LWW,NAIVE").split(",");
    Path work = Path.of(opts.getOrDefault("work", "build/checker-work")).toAbsolutePath();
    HistoryGenerator.Params params = HistoryGenerator.Params.defaults();
    Map<String, Report> results = run(work, params, histories, seed, modes);
    System.out.println(table(results));
    Report full = results.get("FULL");
    if (full != null && full.totalAnomalies() > 0) {
      System.err.println("FULL mode anomalies! failing seeds: " + full.failingSeeds);
      System.exit(1);
    }
    System.exit(0);
  }

  /**
   * Mode names are {@code NAIVE}, {@code LWW}, {@code FULL}, or {@code FULL-noskew} (FULL with the
   * engine's skew bound set to 0 while the generator still produces future-dated events).
   */
  public static Map<String, Report> run(
      Path work, HistoryGenerator.Params params, int histories, long seed, String... modes)
      throws Exception {
    Map<String, Report> results = new LinkedHashMap<>();
    try (Checker checker = new Checker(work)) {
      for (String m : modes) {
        Report total = new Report();
        MergeMode mode = MergeMode.valueOf(m.replace("-noskew", ""));
        long skew = m.endsWith("-noskew") ? 0 : params.maxSkewMs();
        for (int i = 0; i < histories; i++) {
          History h = HistoryGenerator.generate(params, seed + i);
          total.merge(checker.run(h, mode, skew));
        }
        results.put(m, total);
      }
    }
    return results;
  }

  public static String table(Map<String, Report> results) {
    StringBuilder sb = new StringBuilder();
    sb.append("| Mode | Histories | Reads | Wrong reads | % wrong |");
    for (Anomaly a : Anomaly.values()) {
      sb.append(' ').append(a).append(" |");
    }
    sb.append(" Replayed records | Replay ms |\n|---|---|---|---|---|");
    sb.append("---|".repeat(Anomaly.values().length)).append("---|---|\n");
    results.forEach(
        (mode, r) -> {
          sb.append(
              "| %s | %d | %d | %d | %.3f%% |"
                  .formatted(
                      mode, r.histories, r.reads, r.wrongReads, 100.0 * r.wrongReads / r.reads));
          for (Anomaly a : Anomaly.values()) {
            sb.append(' ').append(r.count(a)).append(" |");
          }
          sb.append(" %d | %.1f |\n".formatted(r.replayedRecords, r.replayMicros / 1000.0));
        });
    return sb.toString();
  }
}
