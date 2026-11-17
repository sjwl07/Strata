package io.strata.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@link ObjectStore} on a local directory. Writes go to a temp file and are renamed into place, so
 * a reader never sees a half-written object (the same guarantee S3 gives).
 */
public final class LocalObjectStore implements ObjectStore {

  private final Path root;

  public LocalObjectStore(Path root) throws IOException {
    this.root = Files.createDirectories(root);
  }

  public Path root() {
    return root;
  }

  private Path resolve(String key) {
    Path p = root.resolve(key).normalize();
    if (!p.startsWith(root)) {
      throw new IllegalArgumentException("key escapes store root: " + key);
    }
    return p;
  }

  @Override
  public long put(String key, Path source) throws IOException {
    Path target = resolve(key);
    Files.createDirectories(target.getParent());
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    return Files.size(target);
  }

  @Override
  public void putBytes(String key, byte[] data) throws IOException {
    Path target = resolve(key);
    Files.createDirectories(target.getParent());
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    Files.write(tmp, data);
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public long get(String key, Path target) throws IOException {
    Files.createDirectories(target.toAbsolutePath().getParent());
    Files.copy(resolve(key), target, StandardCopyOption.REPLACE_EXISTING);
    return Files.size(target);
  }

  @Override
  public byte[] getBytes(String key) throws IOException {
    return Files.readAllBytes(resolve(key));
  }

  @Override
  public boolean exists(String key) {
    return Files.exists(resolve(key));
  }

  @Override
  public List<String> list(String prefix) throws IOException {
    try (Stream<Path> s = Files.walk(root)) {
      return s.filter(Files::isRegularFile)
          .map(p -> root.relativize(p).toString().replace('\\', '/'))
          .filter(k -> k.startsWith(prefix) && !k.endsWith(".tmp"))
          .sorted()
          .toList();
    }
  }
}
