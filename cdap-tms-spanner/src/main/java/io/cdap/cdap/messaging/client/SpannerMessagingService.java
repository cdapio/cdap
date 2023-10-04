/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerMessagingService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMessagingService.class);
  public static final String METADATA_TABLE_ID = "messaging-metadata";
  public static final String MESSAGE_TABLE_ID = "messaging-message";
  public static final String NAMESPACE_FIELD = "namespace";
  public static final String TOPIC_FIELD = "topic";
  public static final String PROPERTIES_FIELD = "properties";

  public static final String PAYLOAD_FIELD = "payload";
  public static final String PUBLISH_TS_FIELD = "publish_ts";
  public static final String PAYLOAD_SEQUENCE_ID = "payload_sequence_id";
  public static final String SEQUENCE_ID_FIELD = "sequence_id";

  private final DatabaseClient client = SpannerUtil.getSpannerDbClient();
  private final DatabaseAdminClient adminClient = SpannerUtil.getSpannerDbAdminClient();

  public static final Map<String, Boolean> topicNameSet = new ConcurrentHashMap<>();

  private final ConcurrentLinkedQueue<StoreRequest> batch = new ConcurrentLinkedQueue<>();

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    // if (!topicNameSet.containsKey(topicMetadata.getTopicId().getTopic())) {
    //   synchronized (topicNameSet) {
    //     if (!topicNameSet.containsKey(topicMetadata.getTopicId().getTopic())) {
    //       createTopic(topicMetadata.getTopicId());
    //       topicNameSet.put(topicMetadata.getTopicId().getTopic(), true);
    //     }
    //   }
    // }
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
    OperationFuture<Void, UpdateDatabaseDdlMetadata> future =
        adminClient.updateDatabaseDdl(
            SpannerUtil.instanceId, SpannerUtil.databaseId, Arrays.asList(topicSQL), null);
    try {
      future.get();
    } catch (InterruptedException e) {
      LOG.error("Error when executing %s", topicSQL, e);
    } catch (ExecutionException e) {
      LOG.error("Error when executing %s", topicSQL, e);
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
  public TopicMetadata getTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public MessageFetcher prepareFetch(TopicId topicId) throws TopicNotFoundException, IOException {

    try {
      createTopic(new TopicMetadata(topicId, new HashMap<>()));
    } catch (TopicAlreadyExistsException e) {
      LOG.error("Error creating topic", e);
    }

    return new SpannerMessageFetcher(topicId);
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    long start = System.currentTimeMillis();
    try {
      createTopic(new TopicMetadata(request.getTopicId(), new HashMap<>()));
    } catch (TopicAlreadyExistsException e) {
      LOG.error("Error creating topic", e);
    }

    batch.add(request);

    synchronized (client) {
      if (!batch.isEmpty()) {
        int i = 0;
        List<Mutation> batchCopy = new ArrayList<>(batch.size());
        // We need to batch less than fetch limit since we read for publish_ts >= last_message.publish_ts
        // see fetch for explanation of why we read for publish_ts >= last_message.publish_ts and
        // not publish_ts > last_message.publish_ts
        while (!batch.isEmpty()) {
          StoreRequest headRequest = batch.poll();
          Iterator<byte[]> iterator = headRequest.iterator();
          while (iterator.hasNext()) {
            byte[] payload = iterator.next();

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
}
