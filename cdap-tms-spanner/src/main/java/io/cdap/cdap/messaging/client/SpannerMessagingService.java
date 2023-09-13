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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.RawMessageTableEntry;
import io.cdap.cdap.messaging.store.spanner.SpannerUtil;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.bouncycastle.util.Strings;

public class SpannerMessagingService implements MessagingService {

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

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    String topicSQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s ( %s INT64, %s INT64, %"
                + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), %s BYTES(MAX) )"
                + " PRIMARY KEY (sequence_id, payload_sequence_id, publish_ts), ROW DELETION POLICY"
                + " (OLDER_THAN(publish_ts, INTERVAL 7 DAY));",
            getTableName(topicMetadata.getTopicId()),
            SEQUENCE_ID_FIELD,
            PAYLOAD_SEQUENCE_ID,
            PUBLISH_TS_FIELD,
            PAYLOAD_FIELD);
    adminClient.updateDatabaseDdl(
        SpannerUtil.instanceId, SpannerUtil.databaseId, Arrays.asList(""), null);
  }

  public static String getTableName(TopicId topicId) {
    return Strings.toLowerCase(topicId.getNamespace()) + Strings.toLowerCase(topicId.getTopic());
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
    return new SpannerMessageFetcher(topicId);
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    while (request.iterator().hasNext()) {
      byte[] payload = request.iterator().next();
      Mutation mutation =
          Mutation.newInsertBuilder(MESSAGE_TABLE_ID)
              .set(SEQUENCE_ID_FIELD)
              .to(0)
              .set(PAYLOAD_SEQUENCE_ID)
              .to(0)
              .set(PUBLISH_TS_FIELD)
              .to("spanner.commit_timestamp()")
              .set(PAYLOAD_FIELD)
              .to(ByteArray.copyFrom(payload))
              .build();
      client.write(Arrays.asList(mutation));
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
