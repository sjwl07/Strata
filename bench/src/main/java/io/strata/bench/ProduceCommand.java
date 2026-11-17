package io.strata.bench;

import com.google.protobuf.ByteString;
import io.strata.kafka.KafkaUpdateLog;
import io.strata.proto.v1.UpdateEvent;

/**
 * Writes one stream update to a store's Kafka topic.
 *
 * <pre>bench produce --kafka=HOST:9092 --store=S --partitions=P --key=K (--value=V | --delete=true)
 * </pre>
 */
final class ProduceCommand {
  private ProduceCommand() {}

  static void run(Args a) throws Exception {
    String store = a.s("store", "demo");
    try (KafkaUpdateLog log =
        new KafkaUpdateLog(
            a.s("kafka", "localhost:9092"), "strata-" + store, a.i("partitions", 8), (short) 1)) {
      boolean delete = Boolean.parseBoolean(a.s("delete", "false"));
      UpdateEvent.Builder e =
          UpdateEvent.newBuilder()
              .setKey(ByteString.copyFromUtf8(a.s("key", "")))
              .setTs(a.l("ts", System.currentTimeMillis()))
              .setSourceId(a.i("source", 1))
              .setDelete(delete);
      if (!delete) {
        e.setValue(ByteString.copyFromUtf8(a.s("value", "")));
      }
      long offset = log.append(e.build());
      System.out.println("appended at offset " + offset);
    }
  }
}
