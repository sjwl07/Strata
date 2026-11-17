package io.strata.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.strata.proto.v1.FileRef;
import io.strata.proto.v1.PushType;
import io.strata.proto.v1.VersionManifest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Object-store layout and JSON (de)serialisation for {@link VersionManifest}. */
public final class Manifests {

  private Manifests() {}

  public static String versionPrefix(String store, long version) {
    return "stores/%s/v%06d/".formatted(store, version);
  }

  public static String manifestKey(String store, long version) {
    return versionPrefix(store, version) + "manifest.json";
  }

  public static String sstKey(String store, long version, int partition, String kind) {
    return versionPrefix(store, version) + "p%04d-%s.sst".formatted(partition, kind);
  }

  public static byte[] toJson(VersionManifest m) {
    try {
      return JsonFormat.printer().print(m).getBytes(StandardCharsets.UTF_8);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  public static VersionManifest fromJson(byte[] json) throws IOException {
    VersionManifest.Builder b = VersionManifest.newBuilder();
    JsonFormat.parser().merge(new String(json, StandardCharsets.UTF_8), b);
    return b.build();
  }

  public static VersionManifest load(ObjectStore store, String storeName, long version)
      throws IOException {
    return fromJson(store.getBytes(manifestKey(storeName, version)));
  }

  public static void publish(ObjectStore store, VersionManifest m) throws IOException {
    // Written last: a version "exists" only once its manifest does (all SSTs are already there).
    store.putBytes(manifestKey(m.getStore(), m.getVersion()), toJson(m));
  }

  /**
   * Files a node must ingest, in order, to materialise {@code partition} of {@code m}: the files of
   * the last FULL push followed by every DELTA on top of it. Later files override earlier ones.
   */
  public static List<FileRef> fileChain(ObjectStore store, VersionManifest m, int partition)
      throws IOException {
    List<List<FileRef>> layers = new ArrayList<>();
    VersionManifest cur = m;
    while (true) {
      layers.add(0, filesFor(cur, partition));
      if (cur.getType() != PushType.DELTA) {
        break;
      }
      cur = load(store, cur.getStore(), cur.getBaseVersion());
    }
    List<FileRef> out = new ArrayList<>();
    layers.forEach(out::addAll);
    return out;
  }

  private static List<FileRef> filesFor(VersionManifest m, int partition) {
    return m.getPartitionsList().stream()
        .filter(p -> p.getPartition() == partition)
        .flatMap(p -> p.getFilesList().stream())
        .toList();
  }
}
