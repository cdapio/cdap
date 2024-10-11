package io.cdap.cdap.messaging.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.messaging.SpannerUtil;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerMessagingService extends AbstractIdleService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMessagingService.class);

  private final CConfiguration cConf;

  private Spanner spanner;

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;

  private final ConcurrentLinkedQueue<StoreRequest> batch = new ConcurrentLinkedQueue<>();

  public static final String PAYLOAD_FIELD = "payload";
  public static final String PUBLISH_TS_FIELD = "publish_ts";
  public static final String PAYLOAD_SEQUENCE_ID = "payload_sequence_id";
  public static final String SEQUENCE_ID_FIELD = "sequence_id";

  @Inject
  protected SpannerMessagingService(
      CConfiguration cConf) {
    this.cConf = cConf;

  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    LOG.info("createTopic was invoked");
  }

  private void createTopic(TopicId topicId) {
    String topicSQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s ( %s INT64, %s INT64, %s"
                + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), %s BYTES(MAX) )"
                + " PRIMARY KEY (sequence_id, payload_sequence_id, publish_ts), ROW DELETION POLICY"
                + " (OLDER_THAN(publish_ts, INTERVAL 7 DAY))",
            getTableName(topicId),
            SEQUENCE_ID_FIELD,
            PAYLOAD_SEQUENCE_ID,
            PUBLISH_TS_FIELD,
            PAYLOAD_FIELD);
    LOG.info("createTopic sql {}", topicSQL);
    OperationFuture<Void, UpdateDatabaseDdlMetadata> future =
        adminClient.updateDatabaseDdl(
            SpannerUtil.getInstanceID(cConf), SpannerUtil.getDatabaseID(cConf),
            Collections.singletonList(topicSQL), null);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error when executing {}", topicSQL, e);
    }
  }

  public static String getTableName(TopicId topicId) {
    return topicId.getNamespace() + topicId.getTopic();
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public void deleteTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public Map<String, String> getTopicMetadataProperties(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return null;
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {
    return null;
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    long start = System.currentTimeMillis();
    createTopic(request.getTopicId());

    LOG.info("publish called");
    batch.add(request);

    if (!batch.isEmpty()) {
      int i = 0;
      List<Mutation> batchCopy = new ArrayList<>(batch.size());
      // We need to batch less than fetch limit since we read for publish_ts >= last_message.publish_ts
      // see fetch for explanation of why we read for publish_ts >= last_message.publish_ts and
      // not publish_ts > last_message.publish_ts
      while (!batch.isEmpty()) {
        StoreRequest headRequest = batch.poll();
        for (byte[] payload : headRequest) {
          Mutation mutation =
              Mutation.newInsertBuilder(getTableName(headRequest.getTopicId()))
                  .set(SEQUENCE_ID_FIELD)
                  .to(i++)
                  .set(PAYLOAD_SEQUENCE_ID)
                  .to(0)
                  .set(PUBLISH_TS_FIELD)
                  .to("spanner.commit_timestamp()")
                  .set(PAYLOAD_FIELD)
                  .to(ByteArray.copyFrom(payload))
                  .build();
          LOG.info("mutation to publish {}", mutation);
          batchCopy.add(mutation);
        }

        if (batch.isEmpty() && (i < 50 || System.currentTimeMillis() - start < 50)) {
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            LOG.error("error during sleep", e);
          }
        }
      }
      if (!batchCopy.isEmpty()) {
        try {
          client.write(batchCopy);
        } catch (SpannerException e) {
          LOG.error("Cannot commit mutations ", e);
        }
      }

    }

    return null;
  }

  @Override
  public void storePayload(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public CloseableIterator<RawMessage> fetch(MessageFetchRequest messageFetchRequest)
      throws TopicNotFoundException, IOException {
    String sqlStatement =
        String.format(
            "SELECT %s, %s, UNIX_MICROS(%s), %s FROM %s where (payload_sequence_id>-1 and publish_ts > TIMESTAMP_MICROS(%s)) or"
                + " (payload_sequence_id>-1 and publish_ts = TIMESTAMP_MICROS(%s) and sequence_id > %s) order by"
                + " publish_ts,sequence_id LIMIT %s",
            SpannerMessagingService.SEQUENCE_ID_FIELD,
            SpannerMessagingService.PAYLOAD_SEQUENCE_ID,
            SpannerMessagingService.PUBLISH_TS_FIELD,
            SpannerMessagingService.PAYLOAD_FIELD,
            SpannerMessagingService.getTableName(messageFetchRequest.getTopicId()),
            messageFetchRequest.getStartTime(),
            messageFetchRequest.getStartTime(),
            0, //this.sequenceId
            messageFetchRequest.getLimit());

    try {
      ResultSet resultSet = client.singleUse().executeQuery(Statement.of(sqlStatement));
      return new SpannerResultSetClosableIterator<>(resultSet);
    } catch (Exception ex) {
      LOG.error("Error when fetching {}", sqlStatement, ex);
      throw ex;
    }
  }

  @Override
  protected void startUp() throws Exception {
    spanner = SpannerUtil.getSpannerService(cConf);
    client = SpannerUtil.getSpannerDbClient(cConf, spanner);
    adminClient = SpannerUtil.getSpannerDbAdminClient(spanner);
    LOG.info("Spanner messaging service started.");
  }

  @Override
  protected void shutDown() throws Exception {
    spanner.close();
    LOG.info("Spanner messaging service stopped.");
  }

  public static class SpannerResultSetClosableIterator<RawMessage>
      extends AbstractCloseableIterator<io.cdap.cdap.messaging.spi.RawMessage> {

    private final ResultSet resultSet;

    public SpannerResultSetClosableIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    protected io.cdap.cdap.messaging.spi.RawMessage computeNext() {
      if (!resultSet.next()) {
        return endOfData();
      }

      byte[] id = getMessageId(resultSet.getLong(0), resultSet.getLong(1), resultSet.getLong(2));
      byte[] payload = resultSet.getBytes(3).toByteArray();

      return new io.cdap.cdap.messaging.spi.RawMessage.Builder()
          .setId(id)
          .setPayload(payload)
          .build();
    }

    @Override
    public void close() {
      resultSet.close();
    }
  }

  public static byte[] getMessageId(long sequenceId, long messageSequenceId, long timestamp) {
    byte[] result =
        new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG + Bytes.SIZEOF_SHORT];
    int offset = 0;
    // Implementation copied from MessageId
    offset = Bytes.putLong(result, offset, timestamp);
    offset = Bytes.putShort(result, offset, (short) sequenceId);
    offset = Bytes.putLong(result, offset, 0);
    Bytes.putShort(result, offset, (short) messageSequenceId);
    return result;

    // try {
    //   String messageId= String.format("%s-%s-%s", sequenceId, messageSequenceId, timestamp);
    //   if (messageId.length()%2!=0){
    //     messageId = "0" + messageId;
    //   }
    //
    //   return Bytes.fromHexString(messageId);
    // } catch (Exception e) {
    //   throw new RuntimeException(e);
    // }
  }

}
