package io.strata.node.engine;

import io.strata.common.ObjectStore;
import io.strata.proto.v1.FileRef;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Local copies of pushed SST files, keyed by object key. Pushed files are immutable, so each one is
 * downloaded at most once and then reused, which is what makes delta pushes cheap: the base files
 * are already here. Counts downloaded bytes for the Q1 experiment.
 */
public final class FileCache {

  private final Path dir;
  private final ObjectStore objectStore;
  private final ConcurrentHashMap<String, CompletableFuture<Path>> entries =
      new ConcurrentHashMap<>();
  private final AtomicLong bytesDownloaded = new AtomicLong();

  public FileCache(Path dir, ObjectStore objectStore) throws IOException {
    this.dir = Files.createDirectories(dir);
    this.objectStore = objectStore;
  }

  /** Returns the local path of {@code ref}, downloading it the first time. */
  public Path fetch(FileRef ref) throws IOException {
    boolean[] mine = {false};
    CompletableFuture<Path> f =
        entries.computeIfAbsent(
            ref.getObjectKey(),
            k -> {
              mine[0] = true;
              return new CompletableFuture<>();
            });
    // Whoever inserted the future does the download; everyone else waits on it.
    if (mine[0]) {
      try {
        Path target = dir.resolve(ref.getObjectKey());
        Path tmp = target.resolveSibling(target.getFileName() + ".part");
        long n = objectStore.get(ref.getObjectKey(), tmp);
        Files.move(
            tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        bytesDownloaded.addAndGet(n);
        f.complete(target);
      } catch (IOException | RuntimeException e) {
        entries.remove(ref.getObjectKey(), f);
        f.completeExceptionally(e);
        throw e;
      }
    }
    try {
      return f.join();
    } catch (java.util.concurrent.CompletionException e) {
      if (e.getCause() instanceof IOException io) {
        throw io;
      }
      throw new UncheckedIOException(new IOException(e.getCause()));
    }
  }

  public long bytesDownloaded() {
    return bytesDownloaded.get();
  }

  /**
   * Deletes cached files not in {@code keep}. RocksDB holds its own hard links, so this is safe.
   */
  public void retainOnly(Collection<String> keep) throws IOException {
    Set<String> k = keep.stream().collect(Collectors.toSet());
    for (String key : Set.copyOf(entries.keySet())) {
      if (!k.contains(key)) {
        CompletableFuture<Path> f = entries.remove(key);
        if (f != null && f.isDone() && !f.isCompletedExceptionally()) {
          Files.deleteIfExists(f.join());
        }
      }
    }
  }
}
