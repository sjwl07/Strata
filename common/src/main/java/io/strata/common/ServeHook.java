package io.strata.common;

/**
 * Called by a storage node just before it serves a read, on the serving thread. The fault injector
 * implements this to add latency or simulate pauses; production uses {@link #NONE}.
 */
@FunctionalInterface
public interface ServeHook {

  ServeHook NONE = keys -> {};

  void beforeServe(int keys) throws InterruptedException;
}
