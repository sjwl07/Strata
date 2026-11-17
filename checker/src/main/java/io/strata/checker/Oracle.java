package io.strata.checker;

import io.strata.checker.History.Event;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows the right answer.
 *
 * <p><b>Definition of correct.</b> A version with cutoff W reflects every update with event time at
 * or before W (the batch pipeline reads the source of truth, so it includes updates whose log
 * delivery is late). After that, the store can only know updates delivered through the log. So the
 * correct value of key k, while the version with cutoff W is served and the log has been consumed
 * up to now, is the last-writer-wins maximum over:
 *
 * <pre>  { every event e on k with e.ts <= W }  ∪  { every delivered event e on k with e.ts > W }
 * </pre>
 *
 * (absent if that maximum is a delete or the set is empty). In words: the newest update the system
 * could possibly know about.
 */
public final class Oracle {

  private final List<Event> all; // every event in the history, delivered or not
  private final Map<Integer, List<Event>> delivered = new HashMap<>();

  private final Map<Integer, Event> byId = new HashMap<>();

  public Oracle(List<Event> allEvents) {
    this.all = allEvents;
    allEvents.forEach(e -> byId.put(e.id(), e));
  }

  public Event byId(int id) {
    return byId.get(id);
  }

  public void delivered(Event e) {
    delivered.computeIfAbsent(e.key(), k -> new ArrayList<>()).add(e);
  }

  /** The snapshot the push job builds for cutoff {@code w}: key -> winning live event. */
  public Map<Integer, Event> snapshot(long w) {
    Map<Integer, Event> best = new HashMap<>();
    for (Event e : all) {
      if (e.ts() <= w) {
        best.merge(e.key(), e, (a, b) -> a.stored().compareTo(b.stored()) >= 0 ? a : b);
      }
    }
    best.values().removeIf(Event::delete);
    return best;
  }

  /** The winning event for {@code key} given cutoff {@code w}, or null if none. */
  public Event expected(int key, long w) {
    Event best = null;
    for (Event e : all) {
      if (e.key() == key && e.ts() <= w) {
        best = max(best, e);
      }
    }
    for (Event e : delivered.getOrDefault(key, List.of())) {
      if (e.ts() > w) {
        best = max(best, e);
      }
    }
    return best;
  }

  private static Event max(Event a, Event b) {
    if (a == null) {
      return b;
    }
    return a.stored().compareTo(b.stored()) >= 0 ? a : b;
  }
}
