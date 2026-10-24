package io.strata.node;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.strata.proto.v1.BatchGetRequest;
import io.strata.proto.v1.BatchGetResponse;
import io.strata.proto.v1.GetRequest;
import io.strata.proto.v1.GetResponse;
import io.strata.proto.v1.StorageNodeGrpc;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercise 1 acceptance tests. Copy into storage-node/src/test/java/io/strata/node/ when you start.
 */
class StorageNodeServiceTest {

  private Server server;
  private ManagedChannel channel;
  private StorageNodeGrpc.StorageNodeBlockingStub stub;

  @BeforeEach
  void setUp() throws IOException {
    Map<ByteString, ByteString> data = Map.of(bs("alice"), bs("1"), bs("bob"), bs("2"));
    String name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(new StorageNodeService(data))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    stub = StorageNodeGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void tearDown() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  @Test
  void getReturnsValueForExistingKey() {
    GetResponse response = stub.get(GetRequest.newBuilder().setKey(bs("alice")).build());

    assertThat(response.getFound()).isTrue();
    assertThat(response.getValue()).isEqualTo(bs("1"));
  }

  @Test
  void getReportsMissingKeyAsNotFound() {
    GetResponse response = stub.get(GetRequest.newBuilder().setKey(bs("nobody")).build());

    assertThat(response.getFound()).isFalse();
    assertThat(response.getValue()).isEmpty();
  }

  @Test
  void batchGetPreservesRequestOrderAndMarksMissingKeys() {
    BatchGetResponse response =
        stub.batchGet(
            BatchGetRequest.newBuilder()
                .addKeys(bs("bob"))
                .addKeys(bs("nobody"))
                .addKeys(bs("alice"))
                .build());

    assertThat(response.getEntriesList())
        .extracting(e -> e.getKey().toStringUtf8(), e -> e.getFound(), e -> e.getValue())
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("bob", true, bs("2")),
            org.assertj.core.groups.Tuple.tuple("nobody", false, ByteString.EMPTY),
            org.assertj.core.groups.Tuple.tuple("alice", true, bs("1")));
  }

  @Test
  void emptyKeyIsRejectedWithInvalidArgument() {
    assertThatThrownBy(() -> stub.get(GetRequest.getDefaultInstance()))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  @Test
  void batchGetWithAnEmptyKeyIsRejectedWithInvalidArgument() {
    BatchGetRequest request =
        BatchGetRequest.newBuilder().addKeys(bs("alice")).addKeys(ByteString.EMPTY).build();

    assertThatThrownBy(() -> stub.batchGet(request))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  private static ByteString bs(String s) {
    return ByteString.copyFromUtf8(s);
  }
}
