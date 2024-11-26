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

package io.cdap.cdap.messaging;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
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

/**
 * The Spanner based implementation of {@link MessagingService}.
 */
public class SpannerMessagingService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMessagingService.class);

  private Map<String, String> cConf;

  private Spanner spanner;

  private DatabaseClient client;

  private DatabaseAdminClient adminClient;

  private String instanceId;

  private String databaseId;

  private String projectID;

  private final ConcurrentLinkedQueue<StoreRequest> batch = new ConcurrentLinkedQueue<>();

  public static final String PAYLOAD_FIELD = "payload";
  public static final String PUBLISH_TS_FIELD = "publish_ts";
  public static final String PAYLOAD_SEQUENCE_ID = "payload_sequence_id";
  public static final String SEQUENCE_ID_FIELD = "sequence_id";

  @Override
  public void initialize(MessagingServiceContext context) throws IOException {
    this.cConf = context.getProperties();
    this.databaseId = SpannerUtil.getDatabaseID(cConf);
    this.instanceId = SpannerUtil.getInstanceID(cConf);
    this.projectID = SpannerUtil.getProjectID(cConf);
    this.spanner = SpannerUtil.getSpannerService(projectID);
    this.client = SpannerUtil.getSpannerDbClient(projectID, instanceId, databaseId, spanner);
    this.adminClient = SpannerUtil.getSpannerDbAdminClient(spanner);
    LOG.info("Spanner messaging service started.");
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    createTopic(topicMetadata.getTopicId());
  }

  private void createTopic(TopicId topicId) throws IOException {
    String topicSQL = String.format("CREATE TABLE IF NOT EXISTS %s ( %s INT64, %s INT64, %s"
            + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), %s BYTES(MAX) )"
            + " PRIMARY KEY (sequence_id, payload_sequence_id, publish_ts), ROW DELETION POLICY"
            + " (OLDER_THAN(publish_ts, INTERVAL 7 DAY))", getTableName(topicId), SEQUENCE_ID_FIELD,
        PAYLOAD_SEQUENCE_ID, PUBLISH_TS_FIELD, PAYLOAD_FIELD);
    OperationFuture<Void, UpdateDatabaseDdlMetadata> future = adminClient.updateDatabaseDdl(
        this.instanceId, this.databaseId,
        Collections.singletonList(topicSQL), null);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error when executing {}", topicSQL, e);
      throw new IOException(e);
    }
  }

  public static String getTableName(TopicId topicId) {
    return topicId.getNamespace() + topicId.getTopic();
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public void deleteTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public Map<String, String> getTopicMetadataProperties(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return null;
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    long start = System.currentTimeMillis();
    createTopic(request.getTopicId());

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
          Mutation mutation = Mutation.newInsertBuilder(getTableName(headRequest.getTopicId()))
              .set(SEQUENCE_ID_FIELD).to(i++).set(PAYLOAD_SEQUENCE_ID).to(0).set(PUBLISH_TS_FIELD)
              .to("spanner.commit_timestamp()").set(PAYLOAD_FIELD).to(ByteArray.copyFrom(payload))
              .build();
          batchCopy.add(mutation);
        }

        if (batch.isEmpty() && (i < 50 || System.currentTimeMillis() - start < 50)) {
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            LOG.error("error during sleep", e);
            throw new IOException(e);
          }
        }
      }
      if (!batchCopy.isEmpty()) {
        try {
          client.write(batchCopy);
        } catch (SpannerException e) {
          LOG.error("Cannot commit mutations ", e);
          throw new IOException(e);
        }
      }

    }

    return null;
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

  @Override
  public CloseableIterator<RawMessage> fetch(MessageFetchRequest messageFetchRequest)
      throws TopicNotFoundException, IOException {
    throw new IOException("NOT IMPLEMENTED");
  }
}