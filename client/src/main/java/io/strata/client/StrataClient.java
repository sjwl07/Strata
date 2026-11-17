package io.strata.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.strata.common.Partitioner;
import io.strata.proto.v1.BatchGetRequest;
import io.strata.proto.v1.BatchGetResponse;
import io.strata.proto.v1.ClusterView;
import io.strata.proto.v1.ClusterViewRequest;
import io.strata.proto.v1.ControllerGrpc;
import io.strata.proto.v1.GetRequest;
import io.strata.proto.v1.GetResponse;
import io.strata.proto.v1.KeyValue;
import io.strata.proto.v1.PartitionView;
import io.strata.proto.v1.ReplicaView;
import io.strata.proto.v1.ServerFeedback;
import io.strata.proto.v1.StorageNodeGrpc;
import io.strata.proto.v1.StoreView;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * The Strata read client: routes each key to its partition's replicas (from the controller's
 * cluster view), picks a replica with a {@link ReplicaSelector}, optionally hedges slow requests
 * within a budget, fails over on errors, and cancels losing requests.
 *
 * <pre>{@code
 * try (StrataClient c = new StrataClient(StrataClient.Options.defaults("localhost:7000"))) {
 *   Optional<byte[]> v = c.get("pymk", key).join();
 * }
 * }</pre>
 */
public final class StrataClient implements AutoCloseable {

  /**
   * @param hedgeBudget max hedges as a fraction of primary requests (0 disables hedging)
   * @param hedgePercentile hedge a request still pending after this latency percentile
   * @param minHedgeDelay floor for the adaptive delay
   * @param concurrencyWeight C3's w (about the number of independent clients)
   * @param callbackExecutor where user-visible futures complete (never on gRPC threads)
   */
  public record Options(
      String controllerAddress,
      ReplicaSelector.Strategy strategy,
      double hedgeBudget,
      double hedgePercentile,
      Duration minHedgeDelay,
      Duration deadline,
      Duration viewRefresh,
      double concurrencyWeight,
      Executor callbackExecutor) {

    public static Options defaults(String controllerAddress) {
      return new Options(
          controllerAddress,
          ReplicaSelector.Strategy.C3,
          0.05,
          95,
          Duration.ofMillis(1),
          Duration.ofSeconds(2),
          Duration.ofMillis(200),
          1,
          null);
    }

    public Options withStrategy(ReplicaSelector.Strategy s) {
      return new Options(
          controllerAddress,
          s,
          hedgeBudget,
          hedgePercentile,
          minHedgeDelay,
          deadline,
          viewRefresh,
          concurrencyWeight,
          callbackExecutor);
    }

    public Options withHedgeBudget(double b) {
      return new Options(
          controllerAddress,
          strategy,
          b,
          hedgePercentile,
          minHedgeDelay,
          deadline,
          viewRefresh,
          concurrencyWeight,
          callbackExecutor);
    }
  }

  /** Counters for experiments and dashboards. */
  public static final class Stats {
    public final LongAdder primaries = new LongAdder();
    public final LongAdder hedges = new LongAdder();
    public final LongAdder hedgeWins = new LongAdder();
    public final LongAdder failovers = new LongAdder();
    public final LongAdder cancelled = new LongAdder();
    public final LongAdder failures = new LongAdder();
  }

  /** Replicas of one partition: ready ones are ranked; the others are a last resort. */
  private record Replicas(List<ReplicaStats> ready, List<ReplicaStats> others) {}

  private record Route(int numPartitions, List<Replicas> partitions) {}

  private final Options options;
  private final ManagedChannel controllerChannel;
  private final ControllerGrpc.ControllerBlockingStub controller;
  private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
  private final Map<String, ReplicaStats> replicaStats = new ConcurrentHashMap<>();
  private volatile Map<String, Route> routes = Map.of();
  private final ScheduledExecutorService timer;
  private final ExecutorService ownCallbacks;
  private final Executor callbacks;
  private final ReplicaSelector selector;
  private final TokenBucket hedgeBudget;
  private final Recorder latencyRecorder = new Recorder(3);
  private volatile long hedgeDelayNanos;
  private final Stats stats = new Stats();

  public StrataClient(Options options) {
    this.options = options;
    this.controllerChannel =
        NettyChannelBuilder.forTarget(options.controllerAddress()).usePlaintext().build();
    this.controller = ControllerGrpc.newBlockingStub(controllerChannel);
    ScheduledThreadPoolExecutor t =
        new ScheduledThreadPoolExecutor(
            1,
            r -> {
              Thread th = new Thread(r, "strata-client-timer");
              th.setDaemon(true);
              return th;
            });
    t.setRemoveOnCancelPolicy(true); // cancelled hedge timers must not pile up in the queue
    this.timer = t;
    if (options.callbackExecutor() == null) {
      AtomicInteger n = new AtomicInteger();
      this.ownCallbacks =
          Executors.newFixedThreadPool(
              2,
              r -> {
                Thread th = new Thread(r, "strata-client-cb-" + n.incrementAndGet());
                th.setDaemon(true);
                return th;
              });
      this.callbacks = ownCallbacks;
    } else {
      this.ownCallbacks = null;
      this.callbacks = options.callbackExecutor();
    }
    this.selector = ReplicaSelector.of(options.strategy(), options.concurrencyWeight());
    this.hedgeBudget = new TokenBucket(options.hedgeBudget(), 10);
    this.hedgeDelayNanos = Math.max(options.minHedgeDelay().toNanos(), 10_000_000);
    refreshView();
    timer.scheduleWithFixedDelay(
        this::refreshViewQuietly,
        options.viewRefresh().toMillis(),
        options.viewRefresh().toMillis(),
        TimeUnit.MILLISECONDS);
    timer.scheduleWithFixedDelay(this::refreshHedgeDelay, 1, 1, TimeUnit.SECONDS);
  }

  public Stats stats() {
    return stats;
  }

  public long hedgeDelayNanos() {
    return hedgeDelayNanos;
  }

  // ------------------------------------------------------------------ public API

  /** Value of {@code key} in {@code store}, or empty if absent. */
  public CompletableFuture<Optional<byte[]>> get(String store, byte[] key) {
    Route route = routes.get(store);
    if (route == null) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("unknown store " + store));
    }
    int p = Partitioner.partition(key, route.numPartitions());
    GetRequest req =
        GetRequest.newBuilder().setStore(store).setKey(ByteString.copyFrom(key)).build();
    return call(route.partitions().get(p), stub -> stub.get(req), GetResponse::getFeedback)
        .thenApplyAsync(
            r -> r.getFound() ? Optional.of(r.getValue().toByteArray()) : Optional.<byte[]>empty(),
            callbacks);
  }

  /**
   * Values of many keys (absent keys are missing from the map). Keys are grouped by partition and
   * each group is one hedged, failed-over request, sent in parallel (scatter-gather).
   */
  public CompletableFuture<Map<ByteString, byte[]>> batchGet(String store, List<byte[]> keys) {
    Route route = routes.get(store);
    if (route == null) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("unknown store " + store));
    }
    Map<Integer, BatchGetRequest.Builder> groups = new LinkedHashMap<>();
    for (byte[] k : keys) {
      int p = Partitioner.partition(k, route.numPartitions());
      groups
          .computeIfAbsent(p, x -> BatchGetRequest.newBuilder().setStore(store))
          .addKeys(ByteString.copyFrom(k));
    }
    List<CompletableFuture<BatchGetResponse>> parts = new ArrayList<>();
    groups.forEach(
        (p, b) -> {
          BatchGetRequest req = b.build();
          parts.add(
              call(
                  route.partitions().get(p),
                  stub -> stub.batchGet(req),
                  BatchGetResponse::getFeedback));
        });
    return CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new))
        .thenApplyAsync(
            v -> {
              Map<ByteString, byte[]> out = new HashMap<>();
              for (CompletableFuture<BatchGetResponse> f : parts) {
                for (KeyValue kv : f.join().getEntriesList()) {
                  if (kv.getFound()) {
                    out.put(kv.getKey(), kv.getValue().toByteArray());
                  }
                }
              }
              return out;
            },
            callbacks);
  }

  // ------------------------------------------------------------------ request machinery

  private <T> CompletableFuture<T> call(
      Replicas replicas,
      Function<StorageNodeGrpc.StorageNodeFutureStub, ListenableFuture<T>> rpc,
      Function<T, ServerFeedback> feedbackOf) {
    List<ReplicaStats> ranked = new ArrayList<>(selector.rank(replicas.ready()));
    ranked.addAll(replicas.others());
    if (ranked.isEmpty()) {
      return CompletableFuture.failedFuture(
          Status.UNAVAILABLE.withDescription("no replicas").asRuntimeException());
    }
    stats.primaries.increment();
    hedgeBudget.deposit();
    return new Call<>(ranked, rpc, feedbackOf).start();
  }

  /** One logical request: a primary attempt, maybe a hedge, maybe failovers. */
  private final class Call<T> {
    private final List<ReplicaStats> ranked;
    private final Function<StorageNodeGrpc.StorageNodeFutureStub, ListenableFuture<T>> rpc;
    private final Function<T, ServerFeedback> feedbackOf;
    private final CompletableFuture<T> result = new CompletableFuture<>();
    private final AtomicBoolean done = new AtomicBoolean();
    private final List<ListenableFuture<T>> attempts = new ArrayList<>(); // guarded by this
    private int next; // guarded by this: index of the next replica to try
    private int inFlight; // guarded by this
    private final long start = System.nanoTime();
    private volatile ScheduledFuture<?> hedgeTimer;
    private Throwable lastError;

    Call(
        List<ReplicaStats> ranked,
        Function<StorageNodeGrpc.StorageNodeFutureStub, ListenableFuture<T>> rpc,
        Function<T, ServerFeedback> feedbackOf) {
      this.ranked = ranked;
      this.rpc = rpc;
      this.feedbackOf = feedbackOf;
    }

    CompletableFuture<T> start() {
      sendNext(false);
      if (options.hedgeBudget() > 0 && ranked.size() > 1) {
        hedgeTimer = timer.schedule(this::maybeHedge, hedgeDelayNanos, TimeUnit.NANOSECONDS);
      }
      return result;
    }

    private void maybeHedge() {
      if (done.get()) {
        return;
      }
      synchronized (this) {
        if (next >= ranked.size()) {
          return;
        }
      }
      if (hedgeBudget.tryTake()) {
        stats.hedges.increment();
        sendNext(true);
      }
    }

    private void sendNext(boolean hedge) {
      ReplicaStats r;
      synchronized (this) {
        if (done.get() || next >= ranked.size()) {
          return;
        }
        r = ranked.get(next++);
        inFlight++;
      }
      r.sent();
      long sentAt = System.nanoTime();
      ListenableFuture<T> f;
      try {
        f =
            rpc.apply(
                StorageNodeGrpc.newFutureStub(channel(r.address()))
                    .withDeadlineAfter(options.deadline().toMillis(), TimeUnit.MILLISECONDS));
      } catch (RuntimeException e) {
        r.finished();
        onFailure(r, e, sentAt);
        return;
      }
      synchronized (this) {
        attempts.add(f);
      }
      Futures.addCallback(
          f,
          new FutureCallback<>() {
            @Override
            public void onSuccess(T value) {
              r.finished();
              double ms = (System.nanoTime() - sentAt) / 1e6;
              ServerFeedback fb = feedbackOf.apply(value);
              r.onResponse(ms, fb.getServiceTimeMicros() / 1000.0, fb.getQueueDepth());
              synchronized (Call.this) {
                inFlight--;
              }
              if (done.compareAndSet(false, true)) {
                if (hedge) {
                  stats.hedgeWins.increment();
                }
                finish();
                latencyRecorder.recordValue(
                    Math.min(System.nanoTime() - start, TimeUnit.SECONDS.toNanos(30)));
                result.complete(value);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              r.finished();
              Call.this.onFailure(r, t, sentAt);
            }
          },
          MoreExecutors.directExecutor());
    }

    private void onFailure(ReplicaStats r, Throwable t, long sentAt) {
      boolean cancelledByUs =
          done.get()
              && t instanceof StatusRuntimeException s
              && s.getStatus().getCode() == Status.Code.CANCELLED;
      if (!cancelledByUs) {
        r.onFailure(Math.max(100, (System.nanoTime() - sentAt) / 1e6 * 2));
      }
      boolean retry;
      boolean giveUp;
      synchronized (this) {
        inFlight--;
        lastError = t;
        retry = !done.get() && next < ranked.size() && retryable(t);
        giveUp = !done.get() && !retry && inFlight == 0;
      }
      if (retry) {
        stats.failovers.increment();
        sendNext(false);
      } else if (giveUp && done.compareAndSet(false, true)) {
        finish();
        stats.failures.increment();
        result.completeExceptionally(lastError);
      }
    }

    /** Cancels the hedge timer and any attempt still running (the losers). */
    private void finish() {
      ScheduledFuture<?> ht = hedgeTimer;
      if (ht != null) {
        ht.cancel(false);
      }
      List<ListenableFuture<T>> toCancel;
      synchronized (this) {
        toCancel = new ArrayList<>(attempts);
      }
      for (ListenableFuture<T> a : toCancel) {
        if (!a.isDone() && a.cancel(true)) { // sends RST_STREAM; the server sees CANCELLED
          stats.cancelled.increment();
        }
      }
    }
  }

  private static boolean retryable(Throwable t) {
    if (t instanceof StatusRuntimeException s) {
      Status.Code c = s.getStatus().getCode();
      return c == Status.Code.UNAVAILABLE
          || c == Status.Code.DEADLINE_EXCEEDED
          || c == Status.Code.RESOURCE_EXHAUSTED
          || c == Status.Code.ABORTED;
    }
    return true; // connection-level failures
  }

  // ------------------------------------------------------------------ cluster view & channels

  private ManagedChannel channel(String address) {
    return channels.computeIfAbsent(
        address, a -> NettyChannelBuilder.forTarget(a).usePlaintext().build());
  }

  private void refreshViewQuietly() {
    try {
      refreshView();
    } catch (RuntimeException e) {
      // controller unavailable: keep routing with the last known view
    }
  }

  /** Rebuilds routes from the controller. Ready replicas first, then the rest (for failover). */
  public void refreshView() {
    ClusterView view =
        controller
            .withDeadlineAfter(2, TimeUnit.SECONDS)
            .getClusterView(ClusterViewRequest.getDefaultInstance());
    Map<String, Route> next = new HashMap<>();
    for (StoreView s : view.getStoresList()) {
      List<Replicas> parts = new ArrayList<>();
      for (PartitionView p : s.getPartitionsList()) {
        List<ReplicaStats> ready = new ArrayList<>();
        List<ReplicaStats> others = new ArrayList<>();
        for (ReplicaView r : p.getReplicasList()) {
          ReplicaStats st = replicaStats.computeIfAbsent(r.getAddress(), ReplicaStats::new);
          (r.getReady() ? ready : others).add(st);
        }
        parts.add(new Replicas(List.copyOf(ready), List.copyOf(others)));
      }
      next.put(s.getSpec().getName(), new Route((int) s.getSpec().getNumPartitions(), parts));
    }
    routes = Map.copyOf(next);
  }

  private void refreshHedgeDelay() {
    Histogram h = latencyRecorder.getIntervalHistogram();
    if (h.getTotalCount() >= 100) {
      long p = h.getValueAtPercentile(options.hedgePercentile());
      hedgeDelayNanos = Math.max(options.minHedgeDelay().toNanos(), p);
    }
  }

  @Override
  public void close() {
    timer.shutdownNow();
    if (ownCallbacks != null) {
      ownCallbacks.shutdownNow();
    }
    channels.values().forEach(ManagedChannel::shutdownNow);
    controllerChannel.shutdownNow();
  }
}
