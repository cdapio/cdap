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

package io.cdap.cdap.messaging.store.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.store.AbstractMessageTable;
import io.cdap.cdap.messaging.store.RawMessageTableEntry;
import io.cdap.cdap.messaging.store.RollbackRequest;
import io.cdap.cdap.messaging.store.ScanRequest;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Fields;
import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.tephra.Transaction;

public class SpannerMessageTable extends AbstractMessageTable {

  public static final String METADATA_TABLE_ID = "messaging-metadata";
  public static final String MESSAGE_TABLE_ID = "messaging-message";
  public static final String NAMESPACE_FIELD = "namespace";
  public static final String TOPIC_FIELD = "topic";
  public static final String PROPERTIES_FIELD = "properties";

  public static final String PAYLOAD_FIELD = "payload";
  public static final String PUBLISH_TS_FIELD = "publish_ts";
  public static final String PAYLOAD_SEQUENCE_ID = "payload_sequence_id";
  public static final String SEQUENCE_ID_FIELD = "sequence_id";

  public static final StructuredTableSpecification METADATA_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
          .withId(new StructuredTableId(METADATA_TABLE_ID))
          .withFields(Fields.stringType(NAMESPACE_FIELD), Fields.stringType(PROPERTIES_FIELD))
          .withPrimaryKeys(NAMESPACE_FIELD, TOPIC_FIELD)
          .build();

  public static final StructuredTableSpecification MESSAGE_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
          .withId(new StructuredTableId(MESSAGE_TABLE_ID))
          .withFields(
              Fields.intType(SEQUENCE_ID_FIELD),
              Fields.intType(PAYLOAD_SEQUENCE_ID),
              Fields.longType(PUBLISH_TS_FIELD),
              Fields.bytesType(PAYLOAD_FIELD),
              Fields.stringType(TOPIC_FIELD))
          .withPrimaryKeys(SEQUENCE_ID_FIELD, PAYLOAD_SEQUENCE_ID, PUBLISH_TS_FIELD)
          .build();

  private final DatabaseClient client;

  public SpannerMessageTable() {
    this.client = SpannerUtil.getSpannerDbClient();
  }

  @Override
  protected void persist(Iterator<RawMessageTableEntry> entries) throws IOException {

    List<Mutation> mutations = new ArrayList<>();
    int i = 0;
    while (entries.hasNext()) {
      RawMessageTableEntry entry = entries.next();
      ByteArray payload = ByteArray.copyFrom(entry.getPayload());
      Mutation mutation =
          Mutation.newInsertBuilder(MESSAGE_TABLE_ID)
              .set(SEQUENCE_ID_FIELD)
              .to(i++)
              .set(PAYLOAD_SEQUENCE_ID)
              .to(0)
              .set(PUBLISH_TS_FIELD)
              .to("spanner.commit_timestamp()")
              .set(PAYLOAD_FIELD)
              .to(payload)
              .build();
      mutations.add(mutation);
    }
    client.write(mutations);
  }

  @Override
  protected void rollback(RollbackRequest rollbackRequest) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public CloseableIterator<Entry> fetch(
      TopicMetadata metadata,
      MessageId messageId,
      boolean inclusive,
      final int limit,
      @Nullable final Transaction transaction)
      throws IOException {


    String sqlStatement =
        String.format(
            "SELECT * FROM %s-%s-messaging-metadata where publish_ts > %s",
            metadata.getTopicId().getNamespace(),
            metadata.getTopicId().getTopic(),
            messageId.getPayloadWriteTimestamp());
    try (ResultSet resultSet = client.singleUse().executeQuery(Statement.of(sqlStatement))) {
      while (resultSet.next()){
        long sequenceId = resultSet.getLong(SEQUENCE_ID_FIELD);
        byte[]          payload = resultSet.getBytes(PAYLOAD_FIELD).toByteArray();
        Timestamp timestamp = resultSet.getTimestamp(PUBLISH_TS_FIELD);
      }
    }

    return null;
  }

  @Override
  protected CloseableIterator<RawMessageTableEntry> scan(ScanRequest scanRequest)
      throws IOException {

    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public void close() throws IOException {}

  static final class SpannerTableEntry implements Entry {

    private final TopicId topicId;
    private final byte[] payload;

    private final int sequenceId;
    private final long publishTimestamp;

    public SpannerTableEntry(TopicId topicId, byte[] payload, int sequenceId,
        long publishTimestamp) {
      this.topicId = topicId;
      this.payload = payload;
      this.sequenceId = sequenceId;
      this.publishTimestamp = publishTimestamp;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public int getGeneration() {
      return 0;
    }

    @Override
    public boolean isPayloadReference() {
      return false;
    }

    @Override
    public boolean isTransactional() {
      return false;
    }

    @Override
    public long getTransactionWritePointer() {
      return 0;
    }

    @Nullable
    @Override
    public byte[] getPayload() {
      return payload;
    }

    @Override
    public long getPublishTimestamp() {
      return publishTimestamp;
    }

    @Override
    public short getSequenceId() {
      return 0;
    }
  }
}
