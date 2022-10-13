/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.postgres;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.data.TopicId;
import io.cdap.cdap.messaging.store.MessageTable;
import org.apache.tephra.Transaction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import javax.annotation.Nullable;
import javax.sql.DataSource;

public class PostgresMessageTable implements MessageTable {
  public static final String MESSAGES_TABLE = "messages";
  private final DataSource dataSource;

  PostgresMessageTable(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public final class SimpleMessageEntry implements Entry {
    private final TopicId topicId;
    private final int generation;
    private final byte[] payload;
    private final long publishTimestamp;

    public SimpleMessageEntry(TopicId topicId, int generation, byte[] payload, long publishTimestamp) {
      this.topicId = topicId;
      this.generation = generation;
      this.payload = payload;
      this.publishTimestamp = publishTimestamp;
    }

    @Override
    public TopicId getTopicId() {
      return topicId;
    }

    @Override
    public int getGeneration() {
      return generation;
    }

    @Override
    public boolean isPayloadReference() {
      return payload == null;
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

  public class CloseableListIterator<T extends Entry> implements CloseableIterator<T> {

    ListIterator<T> listIterator;
    public CloseableListIterator(ListIterator<T> listIterator) {
      this.listIterator = listIterator;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
      return listIterator.hasNext();
    }

    @Override
    public T next() {
      return listIterator.next();
    }
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, long startTime, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    TopicId topicId = metadata.getTopicId();
    try (Connection connection = dataSource.getConnection()) {
      String existsSql = "select publish_timestamp, payload from " + MESSAGES_TABLE + " where topic ='" +
                           topicId.getTopic() + "'" + " limit " + limit;
      Statement stmt = connection.createStatement();
      ArrayList<Entry> entries = new ArrayList<>();
      ResultSet rs = stmt.executeQuery(existsSql);
      while (rs.next()) {
        long publishTimestamp = rs.getLong(1);
        byte[] payload = rs.getBytes(2);
        SimpleMessageEntry entry = new SimpleMessageEntry(topicId, 0, payload, publishTimestamp);
        entries.add(entry);
      }
      return new CloseableListIterator<>(entries.listIterator());
    } catch (SQLException e) {
      throw new IOException("Error in getting messages", e);
    }
  }

  @Override
  public CloseableIterator<Entry> fetch(TopicMetadata metadata, MessageId messageId, boolean inclusive, int limit,
                                        @Nullable Transaction transaction) throws IOException {
    // TODO: handle inclusive
    // TODO: handle sequenceId in the messageId. By using only getPublishTimestamp, we may get duplicates
    // for messages that were published in the same millisecond
    return fetch(metadata, messageId.getPublishTimestamp(), limit, transaction);
  }

  @Override
  public void store(Iterator<? extends Entry> entries) throws IOException {
    try (Connection connection = dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      while (entries.hasNext()) {
        Entry entry = entries.next();
        TopicId topicId = entry.getTopicId();
        // TODO handle isPayloadReference. entry.getPayload is not null only if isPayloadReference is false
        String insertSql = String.format("insert into %s (namespace, topic, publish_timestamp, payload, sequence_id) " +
                                           "values " +
                                           "('%s', '%s', %d, decode('%s', 'hex'), %d)",
                                         MESSAGES_TABLE,
                                         topicId.getNamespace(), topicId.getTopic(), entry.getPublishTimestamp(),
                                         entry.isPayloadReference() ? "" : Bytes.toHexString(entry.getPayload()), 1);
        stmt.addBatch(insertSql);
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new IOException("Error in storing messages", e);
    }

  }

  @Override
  public void rollback(TopicMetadata metadata, RollbackDetail rollbackDetail) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}
