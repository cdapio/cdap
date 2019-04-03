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

import io.cdap.cdap.proto.id.TopicId;

import java.util.Iterator;

/**
 * This class represents messages to be store to the messaging system.
 * The message payloads are provide through the {@link Iterator}.
 */
public abstract class StoreRequest implements Iterable<byte[]> {

  private final TopicId topicId;
  private final boolean transactional;
  private final long transactionWritePointer;

  protected StoreRequest(TopicId topicId, boolean transactional, long transactionWritePointer) {
    this.topicId = topicId;
    this.transactional = transactional;
    this.transactionWritePointer = transactionWritePointer;
  }

  public TopicId getTopicId() {
    return topicId;
  }

  /**
   * Returns {@code true} if the message should be published transactionally.
   */
  public boolean isTransactional() {
    return transactional;
  }

  /**
   * Returns the transaction write pointer if the message is going to be published transactionally, that is
   * when {@link #isTransactional()} returns {@code true}.
   */
  public long getTransactionWritePointer() {
    return transactionWritePointer;
  }

  /**
   * Returns {@code true} if there is payload in this request.
   */
  public abstract boolean hasPayload();
}
