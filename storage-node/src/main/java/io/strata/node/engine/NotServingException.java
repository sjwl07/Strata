package io.strata.node.engine;

/** The node can't answer for this key right now (partition not hosted, or no version loaded). */
public final class NotServingException extends Exception {
  public NotServingException(String message) {
    super(message, null, false, false); // no stack trace: this is a routine, hot-path condition
  }
}
