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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.proto.id.TopicId;
import java.io.IOException;
import org.apache.tephra.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerMessageFetcher extends MessageFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerMessageFetcher.class);
  private final TopicId topicId;

  private boolean includeStart = true;
  private Long startTime;
  private Transaction transaction;

  private DatabaseClient client;

  // by default there is virtually no limit
  private int limit = 100;

  public SpannerMessageFetcher(TopicId topicId) {
    this.topicId = topicId;
    this.client = SpannerUtil.getSpannerDbClient();
  }

  @Override
  public MessageFetcher setStartMessage(byte[] startOffset, boolean inclusive) {
    startTime = extractTimestamp(startOffset);
    this.includeStart = inclusive;
    return this;
  }

  @Override
  public MessageFetcher setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public MessageFetcher setTransaction(Transaction transaction) {
    throw new RuntimeException("Not implemented;");
  }

  protected byte[] getStartOffset() {
    throw new RuntimeException("Not implemented;");
  }

  @Override
  public CloseableIterator<RawMessage> fetch() throws TopicNotFoundException, IOException {
    if (!SpannerMessagingService.topicNameSet.contains(topicId.getTopic())){
      return new CloseableIterator<RawMessage>() {
        @Override
        public void close() {

        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public RawMessage next() {
          return null;
        }
      };
    }
    String sqlStatement =
        String.format(
            "SELECT %s, %s, UNIX_MICROS(%s), %s FROM %s where publish_ts > TIMESTAMP_MICROS(%s) LIMIT %s",
            SpannerMessagingService.SEQUENCE_ID_FIELD,
            SpannerMessagingService.PAYLOAD_SEQUENCE_ID,
            SpannerMessagingService.PUBLISH_TS_FIELD,
            SpannerMessagingService.PAYLOAD_FIELD,
            SpannerMessagingService.getTableName(topicId),
            startTime == null ? 0 : startTime,
            limit);
    try {
      ResultSet resultSet = client.singleUse().executeQuery(Statement.of(sqlStatement));
      return new SpannerResultSetClosableIterator<>(resultSet);
    } catch (Exception ex) {
      LOG.error("Error when fetching %s", sqlStatement, ex);
      throw ex;
    }
    // try (ResultSet resultSet = client.singleUse().executeQuery(Statement.of(sqlStatement))) {
    //   while (resultSet.next()) {
    //     long sequenceId = resultSet.getLong(SpannerMessagingService.SEQUENCE_ID_FIELD);
    //     long payloadSequenceId = resultSet.getLong(SpannerMessagingService.PUBLISH_TS_FIELD);
    //     byte[] payload = resultSet.getBytes(SpannerMessagingService.PAYLOAD_FIELD).toByteArray();
    //     Timestamp timestamp = resultSet.getTimestamp(PUBLISH_TS_FIELD);
    //   }
    // }
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

  public static long extractTimestamp(byte[] id) {
    // implementation copied from MessageId

    try {
      return Bytes.toLong(id, 0);
    } catch (Exception e) {
      LOG.error("extractTimestamp error", e);
      return 0;
    }

    // try {
    //   String tmp = Bytes.toHexString(id);
    //   return Long.parseLong(tmp.split("-")[2]);
    // } catch (Exception e) {
    //   LOG.error("extractTimestamp error", e);
    //   return 0;
    // }
  }

  public static class SpannerResultSetClosableIterator<RawMessage>
      implements CloseableIterator<io.cdap.cdap.messaging.data.RawMessage> {

    private final ResultSet resultSet;
    private boolean hasNext;

    public SpannerResultSetClosableIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
      hasNext = resultSet.next();
    }

    @Override
    public void close() {
      resultSet.close();
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public io.cdap.cdap.messaging.data.RawMessage next() {
      if (!hasNext) {
        return null;
      }

      byte[] id = getMessageId(resultSet.getLong(0), resultSet.getLong(1), resultSet.getLong(2));
      byte[] payload = resultSet.getBytes(3).toByteArray();
      hasNext = resultSet.next();

      return new io.cdap.cdap.messaging.data.RawMessage(id, payload);
    }
  }
}
