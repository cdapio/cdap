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

package co.cask.cdap.messaging;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.data.Message;
import co.cask.cdap.messaging.data.MessageId;
import org.apache.tephra.Transaction;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A builder to setup parameters for fetching messages from the messaging system.
 * Sub-class needs to override the {@link #fetch()} method to return a {@link CloseableIterator}
 * for fetching messages.
 */
public abstract class MessageFetcher {

  private MessageId startOffset;
  private boolean includeStart = true;
  private Long startTime;
  private Transaction transaction;

  // by default there is virtually no limit
  private int limit = Integer.MAX_VALUE;

  /**
   * Setup the message fetching starting point based on the given {@link MessageId}. Calling this method
   * will clear the start time set by the {@link #setStartTime(long)} method.
   *
   * @param startOffset the message id to start fetching from.
   * @param inclusive if {@code true}, it will include the message identified by the given {@link MessageId} as the
   *                  first message (if still available in the system); otherwise it won't be included.
   * @return this instance
   */
  public MessageFetcher setStartMessage(MessageId startOffset, boolean inclusive) {
    this.startOffset = startOffset;
    this.includeStart = inclusive;
    this.startTime = null;
    return this;
  }

  /**
   * Setup the message fetching start time (publish time). Calling this method will clear the
   * start offset set by the {@link #setStartMessage(MessageId, boolean)} method.
   *
   * @param startTime timestamp in milliseconds
   * @return this instance
   */
  public MessageFetcher setStartTime(long startTime) {
    if (startTime < 0) {
      throw new IllegalArgumentException("Invalid message fetching start time. Start time must be >= 0");
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
  public MessageFetcher setTransaction(Transaction transaction) {
    this.transaction = transaction;
    return this;
  }

  /**
   * Sets the maximum limit on number of messages to be fetched. By default, this is set to {@code Integer.MAX_VALUE}.
   *
   * @param limit maximum number of messages to be fetched
   * @return this instance
   */
  public MessageFetcher setLimit(int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Invalid message fetching limit. Limit must be > 0");
    }
    this.limit = limit;
    return this;
  }

  @Nullable
  protected MessageId getStartOffset() {
    return startOffset;
  }

  protected boolean isIncludeStart() {
    return includeStart;
  }

  @Nullable
  protected Long getStartTime() {
    return startTime;
  }

  @Nullable
  protected Transaction getTransaction() {
    return transaction;
  }

  protected int getLimit() {
    return limit;
  }

  /**
   * Returns a {@link CloseableIterator} that iterates over messages fetched from the messaging system.
   *
   * @throws TopicNotFoundException if the topic does not exist
   * @throws IOException if it fails to create the iterator
   */
  public abstract CloseableIterator<Message> fetch() throws TopicNotFoundException, IOException;
}
