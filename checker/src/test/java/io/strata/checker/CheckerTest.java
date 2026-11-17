package io.strata.checker;

import static org.assertj.core.api.Assertions.assertThat;

import io.strata.checker.Checker.Anomaly;
import io.strata.checker.Checker.Report;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The CI gate: random histories must produce zero anomalies in FULL mode, and the checker must
 * actually catch the anomalies of the weaker modes (otherwise a green FULL run proves nothing).
 */
class CheckerTest {

  @TempDir Path tmp;

  @Test
  void fullModeHasNoAnomaliesAndWeakerModesDo() throws Exception {
    int histories = Integer.getInteger("checker.histories", 8);
    Map<String, Report> r =
        CheckerMain.run(
            tmp, HistoryGenerator.Params.defaults(), histories, 1000, "FULL", "LWW", "NAIVE");
    System.out.println(CheckerMain.table(r));

    Report full = r.get("FULL");
    assertThat(full.totalAnomalies()).as("failing seeds %s", full.failingSeeds).isZero();
    assertThat(full.reads).isGreaterThan(1000);
    assertThat(full.swaps).isGreaterThan(histories);

    assertThat(r.get("LWW").count(Anomaly.STALE) + r.get("LWW").count(Anomaly.RESURRECTED))
        .isPositive();
    assertThat(r.get("NAIVE").count(Anomaly.REGRESSED)).isPositive();
  }
}
