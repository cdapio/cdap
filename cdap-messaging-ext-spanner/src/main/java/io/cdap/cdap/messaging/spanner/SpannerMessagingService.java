/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.cdap.messaging.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.Credentials;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerMessagingService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMessagingService.class);
  public static final String PAYLOAD_FIELD = "payload";
  public static final String PUBLISH_TS_FIELD = "publish_ts";
  public static final String PUBLISH_TS_MICROS_FIELD = "publish_ts_micros";
  public static final String PAYLOAD_SEQUENCE_ID_FIELD = "payload_sequence_id";
  public static final String PAYLOAD_REMAINING_CHUNKS_FIELD = "payload_remaining_chunks";
  public static final String SEQUENCE_ID_FIELD = "sequence_id";
  public static final String TOPIC_METADATA_TABLE = "topic_metadata";
  public static final String TOPIC_ID_FIELD = "topic_id";
  public static final String PROPERTIES_FIELD = "properties";
  public static final String NAMESPACE_FIELD = "namespace";
  public static final String TOPIC_TABLE_PREFIX = "messaging";

  // Maximum size of data per cell in spanner is 10 MB.
  // Thus, inserting messages with more than 10MB requires chunking them into multiple rows.
  // Keeping max size as 9MB so that we do not touch the spanner threshold.
  private static final int MAX_PAYLOAD_SIZE_IN_BYTES = 9 * 1024 * 1024;

  // Maximum commit size per transaction is 100 MB.
  // Keeping max size as 99MB so that we do not touch the spanner threshold.
  private static final int MAX_BATCH_SIZE_IN_BYTES = 99 * 1024 * 1024;

  private DatabaseClient client;

  private DatabaseAdminClient adminClient;

  private String instanceId;

  private String databaseId;

  private int publishBatchSize;

  private int publishBatchTimeoutMillis;

  private int publishDelayMillis;

  private final ConcurrentLinkedQueue<RequestFuture> batch = new ConcurrentLinkedQueue<>();

  @Override
  public void initialize(MessagingServiceContext context) throws IOException {
    Map<String, String> cConf = context.getProperties();
    this.databaseId = SpannerUtil.getDatabaseID(cConf);
    this.instanceId = SpannerUtil.getInstanceID(cConf);
    String projectID = SpannerUtil.getProjectID(cConf);
    Credentials credentials = SpannerUtil.getCredentials(cConf);

    this.publishBatchSize = Integer.parseInt(cConf.get(SpannerUtil.PUBLISH_BATCH_SIZE));
    this.publishBatchTimeoutMillis = Integer.parseInt(
        cConf.get(SpannerUtil.PUBLISH_BATCH_TIMEOUT_MILLIS));
    this.publishDelayMillis = Integer.parseInt(cConf.get(SpannerUtil.PUBLISH_DELAY_MILLIS));

    Spanner spanner = SpannerUtil.getSpannerService(projectID, credentials);
    this.client = SpannerUtil.getSpannerDbClient(projectID, instanceId, databaseId, spanner);
    this.adminClient = SpannerUtil.getSpannerDbAdminClient(spanner);
    LOG.info("Spanner messaging service started.");

    createTopics(context.getSystemTopics());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    createTopics(ImmutableList.of(topicMetadata));
    LOG.trace("Created topic : {}", topicMetadata.getTopicId().getTopic());
  }

  private void createTopics(List<TopicMetadata> topics) throws IOException {
    if (topics.isEmpty()) {
      LOG.info("No topics were created initially.");
      return;
    }
    List<String> ddlStatements = new ArrayList<>();
    ddlStatements.add(getCreateTopicMetadataDDLStatement());
    for (TopicMetadata topic : topics) {
      LOG.info("Creating topic : {}", topic.getTopicId().getTopic());
      ddlStatements.add(getCreateTopicDDLStatement(topic.getTopicId()));
    }
    executeCreateDDLStatements(ddlStatements);
    updateTopicMetadataTable(topics);
    LOG.info("Created all system topics.");
  }

  private String getCreateTopicMetadataDDLStatement() {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s ( %s STRING(MAX) NOT NULL, %s STRING(MAX), %s JSON ) PRIMARY KEY(%s)",
        TOPIC_METADATA_TABLE, TOPIC_ID_FIELD, NAMESPACE_FIELD, PROPERTIES_FIELD, TOPIC_ID_FIELD);
  }

  /**
   * <p>Key features of the schema:
   *     <ul>
   *         <li>**`sequence_id`:** An arbitrary unique number (0-99) assigned by the publisher.
   *             This helps to distribute writes across the Spanner cluster (avoiding hotspots)
   *             and allows for efficient batching of up to 100 messages per transaction.
   *             The optimal range for `sequence_id` may need to be adjusted based on performance
   *             testing. Smaller ranges generally lead to faster reads but may increase the risk of hotspots.
   *         </li>
   *         <li>**`payload_sequence_id`:** Used to identify message chunks when a message is
   *             larger than 10MB and needs to be split across multiple rows.
   *         </li>
   *         <li>**`publish_ts`:** The commit timestamp obtained from the TrueTime API.
   *             Guaranteed to be monotonically increasing and unique across transactions
   *             that modify the same fields.
   *         </li>
   *         <li>**`payload_remaining_chunks`:** This field helps reassemble large messages
   *             that are split across multiple rows. It indicates how many more parts are needed
   *             to reconstruct the complete message.
   *             A value of 0 means this is the final part.
   *         </li>
   *         <li>**`payload`:** The message body.</li>
   *         <li>Message durability is currently set to 7 days. This means that Messaging service
   *             allows consumers to fetch messages as old as 7 days.</li>
   *     </ul>
   * </p>
   */
  private String getCreateTopicDDLStatement(TopicId topicId) {
    return String.format("CREATE TABLE IF NOT EXISTS %s ( %s INT64, %s INT64, %s"
            + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), %s INT64, %s BYTES(MAX) )"
            + " PRIMARY KEY (%s, %s, %s), ROW DELETION POLICY" + " (OLDER_THAN(%s, INTERVAL 7 DAY))",
        getTableName(topicId), SEQUENCE_ID_FIELD, PAYLOAD_SEQUENCE_ID_FIELD, PUBLISH_TS_FIELD,
        PAYLOAD_REMAINING_CHUNKS_FIELD, PAYLOAD_FIELD, SEQUENCE_ID_FIELD, PAYLOAD_SEQUENCE_ID_FIELD,
        PUBLISH_TS_FIELD, PUBLISH_TS_FIELD);
  }

  private void updateTopicMetadataTable(List<TopicMetadata> topics) throws IOException {
    List<Mutation> mutations = new ArrayList<>();
    for (TopicMetadata topic : topics) {
      Gson gson = new Gson();
      String jsonString = gson.toJson(topic.getProperties());
      Mutation mutation = Mutation.newInsertOrUpdateBuilder(TOPIC_METADATA_TABLE)
          .set(TOPIC_ID_FIELD).to(getTableName(topic.getTopicId())).set(PROPERTIES_FIELD)
          .to(Value.json(jsonString)).set(NAMESPACE_FIELD).to(topic.getTopicId().getNamespace())
          .build();
      mutations.add(mutation);
    }
    try {
      client.write(mutations);
    } catch (SpannerException e) {
      throw new IOException(e);
    }
  }

  private void executeCreateDDLStatements(List<String> ddlStatements) throws IOException {
    try {
      OperationFuture<Void, UpdateDatabaseDdlMetadata> future = adminClient.updateDatabaseDdl(
          this.instanceId, this.databaseId, ddlStatements, null);
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  public static String getTableName(TopicId topicId) {
    return String.join("_", TOPIC_TABLE_PREFIX, topicId.getNamespace(), topicId.getTopic());
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    updateTopicMetadataTable(ImmutableList.of(topicMetadata));
  }

  @Override
  public void deleteTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    try {
      String topicTableName = getTableName(topicId);
      String deleteTopicTableSQL = String.format("DROP TABLE IF EXISTS %s", topicTableName);
      OperationFuture<Void, UpdateDatabaseDdlMetadata> future = adminClient.updateDatabaseDdl(
          this.instanceId, this.databaseId, Collections.singleton(deleteTopicTableSQL), null);
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    Mutation mutation = Mutation.delete(TOPIC_METADATA_TABLE, Key.of(topicId.getTopic()));
    try {
      client.write(Collections.singletonList(mutation));
    } catch (SpannerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Map<String, String> getTopicMetadataProperties(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {

    try (ResultSet resultSet = client.singleUse()
        .read(TOPIC_METADATA_TABLE, KeySet.singleKey(Key.of(getTableName(topicId))),
            Collections.singletonList(PROPERTIES_FIELD))) {
      if (resultSet.next()) {
        String propertiesJson = resultSet.getJson(PROPERTIES_FIELD);
        Gson gson = new Gson();
        return gson.fromJson(propertiesJson, new TypeToken<Map<String, String>>() {
        }.getType());
      } else {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }
    }
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {

    List<TopicId> topics = new ArrayList<>();
    String namespace = namespaceId.getNamespace();
    String topicSQL = String.format("SELECT %s FROM %s WHERE %s = '%s'", TOPIC_ID_FIELD,
        TOPIC_METADATA_TABLE, NAMESPACE_FIELD, namespace);

    try (ResultSet resultSet = client.singleUse().executeQuery(Statement.of(topicSQL))) {
      while (resultSet.next()) {
        String topicId = resultSet.getString(TOPIC_ID_FIELD);
        topics.add(new TopicId(namespace, topicId));
      }
    }
    return ImmutableList.copyOf(topics);
  }

  /**
   * Please refer {@link #getCreateTopicDDLStatement(TopicId)} for schema details. Following table
   * shows how messages would be persisted in the topic tables.
   *
   * <pre>
   * TXN     sequence_id  payload_sequence_id  publish_ts  payload       payload_remaining_chunks
   * TXN1    0            0                    ts1          msg1          0
   * TXN1    1            0                    ts1          msg2_p0       2
   * TXN1    1            1                    ts1          msg2_p1       1
   * TXN1    1            2                    ts1          msg2_p2       0
   * TXN2    0            0                    ts2          msg3          0
   * TXN3    0            0                    ts3          msg4          0
   * </pre>
   */
  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    // All messages within a StoreRequest are published in a single batch to maintain atomicity.
    // Caller should ensure that the request size does not exceed the max allowed size.
    long requestSize = StreamSupport.stream(request.spliterator(), false)
        .mapToLong(payload -> payload.length)
        .sum();
    if (requestSize > MAX_BATCH_SIZE_IN_BYTES) {
      throw new IllegalArgumentException(String.format(
          "Request size %d bytes exceeds the maximum allowed size of %d bytes for Cloud Spanner.",
          requestSize, MAX_BATCH_SIZE_IN_BYTES));
    }

    CompletableFuture<Void> publishFuture = new CompletableFuture<>();
    batch.add(new RequestFuture(request, publishFuture, requestSize));
    long start = System.currentTimeMillis();
    writeData(start);
    // Wait for the corresponding future to finish.
    publishFuture.join();

    //TODO: Add a RollbackDetail implementation that throws exceptions if any of the methods is called.
    return null;
  }

  /**
   * This method efficiently batches and publishes messages to Cloud Spanner, ensuring atomicity and
   * fault tolerance.
   */
  private synchronized void writeData(long start) {
    AtomicInteger sequenceId = new AtomicInteger(0);
    long estimatedBatchSize = 0;
    List<Mutation> mutations = new ArrayList<>(batch.size());
    List<CompletableFuture<Void>> futures = new ArrayList<>(batch.size());

    // Check if adding mutations related to the head will exceed the batch size limit.
    // If exceeding limit, publish the accumulated mutations and then only pick the next item in queue.
    while (!batch.isEmpty() && (estimatedBatchSize + batch.peek().getSize()
        < MAX_BATCH_SIZE_IN_BYTES)) {
      // Ensure that all messages within a StoreRequest are published in a single batch to maintain atomicity.
      // If the batch publish succeeds, all messages are persisted, and the corresponding
      // future is completed successfully.
      // If the batch publish fails, none of the messages are persisted,
      // and the future is completed exceptionally, allowing the caller to retry the entire StoreRequest.
      RequestFuture headRequest = batch.poll();
      List<Mutation> mutationsForRequest = createMutations(headRequest.getStoreRequest(),
          sequenceId);
      mutations.addAll(mutationsForRequest);
      futures.add(headRequest.getFuture());
      estimatedBatchSize += headRequest.getSize();

      // For better performance, wait for more messages to accumulate before committing the batch.
      if (batch.isEmpty() && (sequenceId.get() < publishBatchSize
          || System.currentTimeMillis() - start < publishBatchTimeoutMillis)) {
        try {
          Thread.sleep(publishDelayMillis);
        } catch (InterruptedException e) {
          for (CompletableFuture<Void> future : futures) {
            future.completeExceptionally(e);
          }
        }
      }
    }
    commitPublishMutations(mutations, futures);
  }

  /**
   * This method creates mutations for all the messages that are part of the StoreRequest. Messages
   * with payload larger than 10MB need to be split across multiple rows.
   */
  private List<Mutation> createMutations(StoreRequest request, AtomicInteger sequenceId) {
    List<Mutation> mutations = new ArrayList<>();
    TopicId topicId = request.getTopicId();
    for (byte[] payload : request) {
      int payloadSequenceId = 0;
      int remainingChunksCount = (payload.length - 1) / MAX_PAYLOAD_SIZE_IN_BYTES;
      int length = payload.length;
      int offset = 0;
      while (offset < length) {
        int chunkSize = Math.min(MAX_PAYLOAD_SIZE_IN_BYTES, length - offset);
        ByteBuffer payloadChunk = ByteBuffer.wrap(payload, offset, chunkSize);
        Mutation mutation = createMutation(topicId, sequenceId.get(), payloadSequenceId++,
            remainingChunksCount--, payloadChunk);
        mutations.add(mutation);
        offset += chunkSize;
      }
      sequenceId.set(sequenceId.get() + 1);
    }
    return mutations;
  }

  private Mutation createMutation(TopicId topicId, int sequenceId, int payloadSequenceId,
      int remainingParts, ByteBuffer payload) {
    Mutation mutation = Mutation.newInsertBuilder(getTableName(topicId)).set(SEQUENCE_ID_FIELD)
        .to(sequenceId).set(PAYLOAD_SEQUENCE_ID_FIELD).to(payloadSequenceId).set(PUBLISH_TS_FIELD)
        .to("spanner.commit_timestamp()").set(PAYLOAD_FIELD).to(ByteArray.copyFrom(payload))
        .set(PAYLOAD_REMAINING_CHUNKS_FIELD).to(remainingParts).build();
    LOG.trace("mutation to publish {}", mutation);
    return mutation;
  }

  private void commitPublishMutations(List<Mutation> mutations,
      List<CompletableFuture<Void>> futures) {
    if (!mutations.isEmpty()) {
      try {
        client.write(mutations);
        LOG.trace("published mutations : {}", mutations.size());
        for (CompletableFuture<Void> future : futures) {
          future.complete(null);
        }
      } catch (SpannerException e) {
        for (CompletableFuture<Void> future : futures) {
          future.completeExceptionally(e);
        }
      }
    }
  }

  @Override
  public void storePayload(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  /**
   * Fetches messages from the specified topic.
   *
   * <p>The fetch operation utilizes the following protocol:
   *     <ul>
   *         <li>**MessageId Construction:**
   *             MessageId is constructed as the concatenation of `sequence_id` and `publish_ts`.
   *             This guarantees the following: If the `publish_ts` of message A is less than the
   *            `publish_ts` of message B, then message A is delivered before message B.
   *         </li>
   *         <li>**Fetch Logic:**
   *             - If the provided `messageId` exists in-memory, the iterator pointing to the next message is returned.
   *             - Otherwise, a Spanner read operation is issued to fetch messages
   *               with `publish_ts` greater than the `publish_ts` in the provided `messageId`.
   *             - Upon reading rows, chunked messages are reconstructed as a single message and are held in memory.
   *         </li>
   *     </ul>
   * </p>
   */
  @Override
  public CloseableIterator<RawMessage> fetch(MessageFetchRequest messageFetchRequest)
      throws TopicNotFoundException, IOException {
    // If the client is fetching for the first time ever,
    // they should fetch data for publish_ts > 0 and sequence_id > -1.
    Long startTime = 0L;
    if (messageFetchRequest.getStartTime() != null) {
      startTime = messageFetchRequest.getStartTime();
    }
    short sequenceId = -1;
    // MessageId is constructed as the concatenation of PublishTs and SequenceNo columns.
    // These fields are set in the offset field of the message fetch request.
    byte[] id = messageFetchRequest.getStartOffset();
    if (id != null) {
      int offset = 0;
      startTime = Bytes.toLong(id, offset);
      offset += Bytes.SIZEOF_LONG;
      sequenceId = Bytes.toShort(id, offset);
    }

    LOG.trace("Fetching message from topic : {} with start time : {} sequenceId : {}",
        messageFetchRequest.getTopicId().getTopic(), startTime, sequenceId);
    // publish_ts > TIMESTAMP_MICROS(startTime)
    // or
    // publish_ts = TIMESTAMP_MICROS(startTime) and sequence_id > sequenceId
    // order by
    // publish_ts, sequence_id, payload_sequence_id
    String sqlStatement = String.format(
        "SELECT %s, %s, UNIX_MICROS(%s) %s, %s, %s FROM %s where (%s > TIMESTAMP_MICROS(%s)) or"
            + " (%s = TIMESTAMP_MICROS(%s) and %s > %s) order by" + " %s, %s, %s LIMIT %s",
        SEQUENCE_ID_FIELD, PAYLOAD_SEQUENCE_ID_FIELD, PUBLISH_TS_FIELD, PUBLISH_TS_MICROS_FIELD,
        PAYLOAD_REMAINING_CHUNKS_FIELD, PAYLOAD_FIELD,
        getTableName(messageFetchRequest.getTopicId()), PUBLISH_TS_FIELD, startTime,
        PUBLISH_TS_FIELD, startTime, SEQUENCE_ID_FIELD, sequenceId, PUBLISH_TS_FIELD,
        SEQUENCE_ID_FIELD, PAYLOAD_SEQUENCE_ID_FIELD, messageFetchRequest.getLimit());
    try {
      ResultSet resultSet = client.singleUse().executeQuery(Statement.of(sqlStatement));
      return new SpannerResultSetClosableIterator<>(resultSet);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  public static class SpannerResultSetClosableIterator<RawMessage> extends
      AbstractCloseableIterator<io.cdap.cdap.messaging.spi.RawMessage> {

    private final ResultSet resultSet;

    public SpannerResultSetClosableIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    protected io.cdap.cdap.messaging.spi.RawMessage computeNext() {
      if (!resultSet.next()) {
        return endOfData();
      }
      long remainingParts = resultSet.getLong(PAYLOAD_REMAINING_CHUNKS_FIELD);
      ByteArrayOutputStream payload = new ByteArrayOutputStream();
      try {
        payload.write(resultSet.getBytes(PAYLOAD_FIELD).toByteArray());
        while (remainingParts > 0) {
          if (!resultSet.next()) {
            // Note : result.next() moves the pointer to the next row in Spanner fetch results.
            LOG.trace("Fetch batch is incomplete. Looking for more {} rows", remainingParts);
            return endOfData();
          }
          payload.write(resultSet.getBytes(PAYLOAD_FIELD).toByteArray());
          remainingParts--;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // We always set payload sequence id in fetch() as 0,
      // since we send back just one message after re-assembling all the payload chunks.
      byte[] id = getMessageId(resultSet.getLong(SEQUENCE_ID_FIELD), 0,
          resultSet.getLong(PUBLISH_TS_MICROS_FIELD));
      return new io.cdap.cdap.messaging.spi.RawMessage.Builder().setId(id)
          .setPayload(payload.toByteArray()).build();
    }

    @Override
    public void close() {
      resultSet.close();
    }
  }

  @VisibleForTesting
  static byte[] getMessageId(long sequenceId, long payloadSequenceId, long timestamp) {
    LOG.trace("sequenceId {} payloadSequenceId {} timestamp {}", sequenceId, payloadSequenceId,
        timestamp);
    byte[] result = new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG
        + Bytes.SIZEOF_SHORT];
    int offset = 0;
    // Implementation copied from MessageId.
    offset = Bytes.putLong(result, offset, timestamp);
    offset = Bytes.putShort(result, offset, (short) sequenceId);
    // This 0 corresponds to the write timestamp which we do not maintain in case of spanner messaging service.
    offset = Bytes.putLong(result, offset, 0);
    Bytes.putShort(result, offset, (short) payloadSequenceId);
    return result;
  }

  // Helper class to hold the publish request and its future.
  private static class RequestFuture {

    private final StoreRequest storeRequest;
    private final CompletableFuture<Void> future;
    private final long size;

    private RequestFuture(StoreRequest storeRequest, CompletableFuture<Void> future, long size) {
      this.storeRequest = storeRequest;
      this.future = future;
      this.size = size;
    }

    private StoreRequest getStoreRequest() {
      return storeRequest;
    }

    private long getSize() {
      return size;
    }

    private CompletableFuture<Void> getFuture() {
      return future;
    }
  }

}
