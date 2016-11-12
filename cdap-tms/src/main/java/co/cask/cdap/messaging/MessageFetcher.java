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

/**
 * A builder to setup parameters for fetching messages from the messaging system.
 */
public abstract class MessageFetcher {

  protected MessageId startOffset;
  protected boolean includeStart;
  protected Long startTime;
  protected Transaction transaction;

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

  public abstract CloseableIterator<Message> fetch() throws TopicNotFoundException;
}
