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

package io.cdap.cdap.messaging;

import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.proto.id.TopicId;
import org.apache.tephra.Transaction;

public class DefaultMessageFetchRequest implements MessageFetchRequest {

  private final TopicId topicId;
  private byte[] startOffset;
  private boolean includeStart = true;
  private Long startTime;
  private Transaction transaction;

  // by default there is virtually no limit
  private int limit = Integer.MAX_VALUE;

  private DefaultMessageFetchRequest(
      TopicId topicId,
      byte[] startOffset,
      boolean includeStart,
      Long startTime,
      Transaction transaction,
      int limit) {
    this.topicId = topicId;
    this.startOffset = startOffset;
    this.includeStart = includeStart;
    this.startTime = startTime;
    this.transaction = transaction;
    this.limit = limit;
  }

  @Override
  public TopicId getTopicId() {
    return topicId;
  }

  @Override
  public byte[] getStartOffset() {
    return startOffset;
  }

  @Override
  public boolean isIncludeStart() {
    return includeStart;
  }

  @Override
  public Long getStartTime() {
    return startTime;
  }

  @Override
  public Transaction getTransaction() {
    return transaction;
  }

  @Override
  public int getLimit() {
    return limit;
  }

  public static class Builder {

    private TopicId topicId;
    private byte[] startOffset;
    private boolean includeStart = true;
    private Long startTime;
    private Transaction transaction;

    // by default there is virtually no limit
    private int limit = Integer.MAX_VALUE;

    public Builder setTopicId(TopicId topicId) {
      this.topicId = topicId;
      return this;
    }

    /**
     * Setup the message fetching starting point based on the given message id. Calling this method
     * will clear the start time set by the {@link #setStartTime(long)} method.
     *
     * @param startOffset the message id to start fetching from.
     * @param inclusive if {@code true}, it will include the message identified by the given message
     *     id as the first message (if still available in the system); otherwise it won't be
     *     included.
     * @return this instance
     */
    public Builder setStartMessage(byte[] startOffset, boolean inclusive) {
      this.startOffset = startOffset;
      this.includeStart = inclusive;
      this.startTime = null;
      return this;
    }

    /**
     * Setup the message fetching start time (publish time). Calling this method will clear the
     * start offset set by the {@link #setStartMessage(byte[], boolean)} method.
     *
     * @param startTime timestamp in milliseconds
     * @return this instance
     */
    public Builder setStartTime(long startTime) {
      if (startTime < 0) {
        throw new IllegalArgumentException(
            "Invalid message fetching start time. Start time must be >= 0");
      }
      this.startTime = startTime;
      this.startOffset = null;
      return this;
    }

    /**
     * Sets the transaction to use for fetching. It is for transactional consumption.
     *
     * @param transaction the transaction to use for reading messages
     * @return this instance
     */
    public Builder setTransaction(Transaction transaction) {
      this.transaction = transaction;
      return this;
    }

    /**
     * Sets the maximum limit on number of messages to be fetched. By default, this is set to {@code
     * Integer.MAX_VALUE}.
     *
     * @param limit maximum number of messages to be fetched
     * @return this instance
     */
    public Builder setLimit(int limit) {
      if (limit <= 0) {
        throw new IllegalArgumentException("Invalid message fetching limit. Limit must be > 0");
      }
      this.limit = limit;
      return this;
    }

    public MessageFetchRequest build() {
      return new DefaultMessageFetchRequest(
          topicId, startOffset, includeStart, startTime, transaction, limit);
    }
  }
}
