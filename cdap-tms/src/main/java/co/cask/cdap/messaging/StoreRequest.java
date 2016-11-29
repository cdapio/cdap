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

import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * This class represents messages to be store to the messaging system.
 * The message payloads are provide through the {@link Iterator}.
 */
public abstract class StoreRequest extends AbstractIterator<byte[]> {

  private final TopicId topicId;
  private final boolean transactional;
  private final long transactionWritePointer;
  private boolean computedFirst;

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

  @Override
  protected final byte[] computeNext() {
    byte[] next = doComputeNext();
    if (!computedFirst && next == null && !isTransactional()) {
      throw new IllegalStateException("Invalid payload. Empty message is only allowed for transactional message");
    }
    computedFirst = true;
    return next == null ? endOfData() : next;
  }

  /**
   * Returns the next message payloads or return {@code null} to signal the end of payload messages. If this
   * method returns {@code null} on the first time being called, it means this {@link StoreRequest} represents
   * a message table entry that reference to payload table. It requires {@link #isTransactional()} returns
   * {@code true} and {@link #getTransactionWritePointer()} should return the transaction write pointer for
   * messages stored in the Payload Table prior to this call.
   */
  @Nullable
  protected abstract byte[] doComputeNext();
}
