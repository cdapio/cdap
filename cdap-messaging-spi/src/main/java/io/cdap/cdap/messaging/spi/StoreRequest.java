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

package io.cdap.cdap.messaging.spi;

import io.cdap.cdap.proto.id.TopicId;
import java.util.Iterator;

/**
 * This class represents messages to be store to the messaging system. The message payloads are
 * provide through the {@link Iterator}.
 */
public interface StoreRequest extends Iterable<byte[]> {

  TopicId getTopicId();

  /** Returns {@code true} if the message should be published transactionally. */
  @Deprecated
  boolean isTransactional();

  /**
   * Returns the transaction write pointer if the message is going to be published transactionally,
   * that is when {@link #isTransactional()} returns {@code true}.
   */
  @Deprecated
  long getTransactionWritePointer();

  /** Returns {@code true} if there is payload in this request. */
  @Deprecated
  boolean hasPayload();
}
