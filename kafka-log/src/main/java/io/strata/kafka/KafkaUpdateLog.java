package io.strata.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import io.strata.common.UpdateLog;
import io.strata.proto.v1.UpdateEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * {@link UpdateLog} on a Kafka topic with one Kafka partition per store partition.
 *
 * <p>Design points:
 *
 * <ul>
 *   <li>The topic uses {@code message.timestamp.type=LogAppendTime}, so record timestamps are
 *       broker append times. That's the clock {@link #offsetForTime} needs for replay (ADR 0004).
 *       Event time travels inside the {@link UpdateEvent}.
 *   <li>No consumer groups: each replica assigns partitions manually and tracks its own offsets,
 *       because every replica must read <i>every</i> record (Kafka as the replication log), not
 *       share the work as a group would.
 *   <li>{@code KafkaConsumer} isn't thread-safe, so each (thread, partition) pair gets its own
 *       consumer. A partition's reads all come from that partition's single worker thread. Metadata
 *       calls (end offsets, offset-for-time) share one consumer under a lock.
 * </ul>
 */
public final class KafkaUpdateLog implements UpdateLog {

  private final String topic;
  private final int numPartitions;
  private final Properties base;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaConsumer<byte[], byte[]> metadata;
  private final Map<String, KafkaConsumer<byte[], byte[]>> readers = new ConcurrentHashMap<>();

  public KafkaUpdateLog(String bootstrapServers, String topic, int numPartitions, short rf)
      throws InterruptedException, ExecutionException {
    this.topic = topic;
    this.numPartitions = numPartitions;
    this.base = new Properties();
    base.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    createTopicIfMissing(rf);

    Properties p = new Properties();
    p.putAll(base);
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    p.put(ProducerConfig.LINGER_MS_CONFIG, "1");
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    this.producer = new KafkaProducer<>(p);
    this.metadata = newConsumer();
  }

  private void createTopicIfMissing(short rf) throws InterruptedException, ExecutionException {
    try (Admin admin = Admin.create(base)) {
      NewTopic t =
          new NewTopic(topic, numPartitions, rf)
              .configs(Map.of("message.timestamp.type", "LogAppendTime"));
      try {
        admin.createTopics(List.of(t)).all().get();
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw e;
        }
      }
    }
  }

  private KafkaConsumer<byte[], byte[]> newConsumer() {
    Properties c = new Properties();
    c.putAll(base);
    c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
    c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return new KafkaConsumer<>(c);
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  public long append(int partition, UpdateEvent event) {
    try {
      RecordMetadata md =
          producer
              .send(
                  new ProducerRecord<>(
                      topic, partition, event.getKey().toByteArray(), event.toByteArray()))
              .get();
      return md.offset();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("append failed", e.getCause());
    }
  }

  @Override
  public List<LogRecord> read(int partition, long fromOffset, int max, Duration timeout) {
    TopicPartition tp = new TopicPartition(topic, partition);
    KafkaConsumer<byte[], byte[]> c =
        readers.computeIfAbsent(
            Thread.currentThread().threadId() + "/" + partition,
            k -> {
              KafkaConsumer<byte[], byte[]> nc = newConsumer();
              nc.assign(List.of(tp));
              return nc;
            });
    if (c.position(tp) != fromOffset) {
      c.seek(tp, fromOffset);
    }
    List<LogRecord> out = new ArrayList<>();
    for (ConsumerRecord<byte[], byte[]> r : c.poll(timeout).records(tp)) {
      if (out.size() == max) {
        c.seek(tp, r.offset()); // hand the rest to the next call
        break;
      }
      try {
        out.add(new LogRecord(r.offset(), r.timestamp(), UpdateEvent.parseFrom(r.value())));
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException("corrupt record at offset " + r.offset(), e);
      }
    }
    return out;
  }

  @Override
  public synchronized long endOffset(int partition) {
    TopicPartition tp = new TopicPartition(topic, partition);
    return metadata.endOffsets(List.of(tp)).get(tp);
  }

  @Override
  public synchronized long offsetForTime(int partition, long appendTimeMs) {
    TopicPartition tp = new TopicPartition(topic, partition);
    OffsetAndTimestamp o = metadata.offsetsForTimes(Map.of(tp, Math.max(0, appendTimeMs))).get(tp);
    if (o != null) {
      return o.offset();
    }
    return metadata.endOffsets(List.of(tp)).get(tp);
  }

  @Override
  public void close() {
    producer.close();
    synchronized (this) {
      metadata.close();
    }
    // Reader consumers belong to their threads; close them best-effort here at shutdown.
    readers.values().forEach(KafkaConsumer::close);
  }
}
