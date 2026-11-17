package io.strata.common;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Minimal blob store for pushed SST files and manifests, in the shape of S3/MinIO: immutable
 * objects addressed by string keys. See ADR 0001 for why the default is a local directory.
 */
public interface ObjectStore {

  /** Uploads a local file. Returns the number of bytes stored. */
  long put(String key, Path source) throws IOException;

  void putBytes(String key, byte[] data) throws IOException;

  /** Downloads an object into {@code target}. Returns the number of bytes transferred. */
  long get(String key, Path target) throws IOException;

  byte[] getBytes(String key) throws IOException;

  boolean exists(String key) throws IOException;

  /** Keys starting with {@code prefix}, sorted. */
  List<String> list(String prefix) throws IOException;
}
