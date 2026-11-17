package io.strata.push;

import java.io.IOException;

/**
 * One full snapshot of a dataset: every live key with its value. Must be re-iterable (the push job
 * scans it once per partition group, which keeps memory at O(N / partitions)).
 */
public interface Dataset {

  void forEach(Visitor visitor) throws IOException;

  /** Only the rows of one partition. Datasets that can skip rows cheaply should override. */
  default void forEachInPartition(int partition, int numPartitions, Visitor visitor)
      throws IOException {
    forEach(
        (k, v) -> {
          if (io.strata.common.Partitioner.partition(k, numPartitions) == partition) {
            visitor.visit(k, v);
          }
        });
  }

  @FunctionalInterface
  interface Visitor {
    void visit(byte[] key, byte[] value) throws IOException;
  }
}
