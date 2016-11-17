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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.proto.id.TopicId;

import java.io.IOException;

/**
 * Contains common logic for implementation of {@link PayloadTable}
 */
public abstract class AbstractPayloadTable implements PayloadTable {

  private long writeTimestamp;
  private short pSeqId;

  @Override
  public void delete(TopicId topicId, long transactionWritePointer) throws IOException {
    byte[] topic = MessagingUtils.toRowKeyPrefix(topicId);
    byte[] startRow = new byte[topic.length + Bytes.SIZEOF_LONG];
    Bytes.putBytes(startRow, 0, topic, 0, topic.length);
    Bytes.putLong(startRow, topic.length, transactionWritePointer);
    byte[] stopRow = Bytes.stopKeyForPrefix(startRow);
    performDelete(startRow, stopRow);
  }

  protected abstract void performDelete(byte[] startRow, byte[] stopRow) throws IOException;
}
