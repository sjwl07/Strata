package io.strata.push;

import io.strata.common.LocalObjectStore;
import io.strata.proto.v1.VersionManifest;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Command line for the push job.
 *
 * <pre>
 * push-job --objects=DIR --store=NAME --partitions=P --version=N --watermark=MS
 *          (--input=FILE.tsv | --synthetic-keys=K [--value-size=B] [--seed=S] [--snapshot=I]
 *                                                 [--change=F] [--delete=F])
 *          [--delta-from=FILE.tsv | --delta-base-version=N-1]
 * </pre>
 *
 * For synthetic deltas the previous snapshot is regenerated from the same parameters with {@code
 * snapshot - 1}.
 */
public final class PushJobMain {
  private PushJobMain() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> o = new HashMap<>();
    for (String a : args) {
      o.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
    }
    String store = req(o, "store");
    int partitions = Integer.parseInt(req(o, "partitions"));
    long version = Long.parseLong(req(o, "version"));
    long watermark = Long.parseLong(o.getOrDefault("watermark", "" + System.currentTimeMillis()));
    LocalObjectStore objects = new LocalObjectStore(Path.of(req(o, "objects")));
    PushJob job =
        new PushJob(
            new PushJob.Config(
                store,
                partitions,
                objects,
                Path.of(o.getOrDefault("work", System.getProperty("java.io.tmpdir"))),
                Integer.parseInt(o.getOrDefault("parallelism", "2"))));

    Dataset next;
    Dataset prev = null;
    if (o.containsKey("input")) {
      next = new TsvDataset(Path.of(o.get("input")));
      if (o.containsKey("delta-from")) {
        prev = new TsvDataset(Path.of(o.get("delta-from")));
      }
    } else {
      long keys = Long.parseLong(req(o, "synthetic-keys"));
      int valueSize = Integer.parseInt(o.getOrDefault("value-size", "100"));
      long seed = Long.parseLong(o.getOrDefault("seed", "1"));
      int snapshot = Integer.parseInt(o.getOrDefault("snapshot", "0"));
      double change = Double.parseDouble(o.getOrDefault("change", "0.01"));
      double delete = Double.parseDouble(o.getOrDefault("delete", "0"));
      next = new SyntheticDataset(keys, valueSize, seed, snapshot, change, delete);
      if (o.containsKey("delta-base-version") && snapshot > 0) {
        prev = new SyntheticDataset(keys, valueSize, seed, snapshot - 1, change, delete);
      }
    }
    VersionManifest m;
    if (prev != null) {
      long base = Long.parseLong(o.getOrDefault("delta-base-version", "" + (version - 1)));
      m = job.pushDelta(prev, next, base, version, watermark);
    } else {
      m = job.pushFull(next, version, watermark);
    }
    System.out.printf(
        "published %s v%d (%s): %d records, %d bytes, %d ms%n",
        store,
        version,
        m.getType(),
        m.getStats().getRecordsWritten(),
        m.getStats().getBytesWritten(),
        m.getStats().getBuildMillis());
  }

  private static String req(Map<String, String> o, String k) {
    String v = o.get(k);
    if (v == null) {
      throw new IllegalArgumentException("missing --" + k + "=...");
    }
    return v;
  }
}
