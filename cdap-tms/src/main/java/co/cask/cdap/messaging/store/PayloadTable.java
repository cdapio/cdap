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

package co.cask.cdap.messaging.store;

import co.cask.cdap.messaging.data.PayloadMessage;
import co.cask.cdap.proto.id.TopicId;

import java.util.List;

/**
 * Table to store transactional messages.
 */
public interface PayloadTable {

  /**
   *
   * @param topicId
   * @param transactionWritePointer
   * @param limit
   * @return
   */
  List<PayloadMessage> fetch(TopicId topicId, long transactionWritePointer, int limit);

  /**
   *
   * @param topicId
   * @param transactionWritePointer
   * @param timestampMs
   * @param sequenceId
   * @param limit
   * @return
   */
  List<PayloadMessage> fetch(TopicId topicId, long transactionWritePointer, long timestampMs, short sequenceId,
                             int limit);

  /**
   * Store messages with a transaction write pointer
   *
   * @param topicId
   * @param transactionWritePointer
   * @param timestampMs
   * @param startSeqId
   * @param messages
   */
  void store(TopicId topicId, long transactionWritePointer, long timestampMs, short startSeqId, List<byte[]> messages);

  /**
   * Delete all the messages stored with the given transactionWritePointer
   *
   * @param topicId
   * @param transactionWritePointer
   */
  void delete(TopicId topicId, long transactionWritePointer);
}
