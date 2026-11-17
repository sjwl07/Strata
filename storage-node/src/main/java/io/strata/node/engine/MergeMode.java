package io.strata.node.engine;

/**
 * How stream updates are combined with batch data. The three modes exist so the Q2 experiment can
 * measure what each mechanism buys (see docs/adr/0003-merge-semantics.md).
 */
public enum MergeMode {
  /** Apply every update blindly in arrival order; no replay when a new version is swapped in. */
  NAIVE,
  /** Last-writer-wins by (ts, sourceId), but no watermark filter and no replay on swap. */
  LWW,
  /** LWW, plus drop updates at or below the version's watermark, plus replay before swap. */
  FULL
}
