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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Base class for Message Table tests.
 */
public abstract class MessageTableTest {

  @Test
  public void testStore() throws Exception {
    try (MessageTable table = getTable()) {
      final TopicId id = NamespaceId.DEFAULT.topic("t1");
      List<MessageTable.Entry> entryList = new ArrayList<>();
      final byte[] payload = Bytes.toBytes("data");
      final long writePtr = 100L;
      entryList.add(new MessageTable.Entry() {
        @Override
        public TopicId getTopicId() {
          return id;
        }

        @Override
        public boolean isPayloadReference() {
          return false;
        }

        @Override
        public boolean isTransactional() {
          return true;
        }

        @Override
        public long getTransactionWritePointer() {
          return writePtr;
        }

        @Nullable
        @Override
        public byte[] getPayload() {
          return payload;
        }

        @Override
        public long getPublishTimestamp() {
          return 0;
        }

        @Override
        public short getSequenceId() {
          return 0;
        }
      });
      table.store(entryList.iterator());
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      CloseableIterator<MessageTable.Entry> iterator = table.fetch(id, new MessageId(messageId), true, 50, null);
      Assert.assertTrue(iterator.hasNext());
      MessageTable.Entry entry = iterator.next();
      Assert.assertArrayEquals(payload, entry.getPayload());
      Assert.assertFalse(iterator.hasNext());
      iterator = table.fetch(id, 0, 50, null);
      entry = iterator.next();
      Assert.assertArrayEquals(payload, entry.getPayload());
      Assert.assertFalse(iterator.hasNext());
      table.delete(id, writePtr);
      iterator = table.fetch(id, new MessageId(messageId), true, 50, null);
      Assert.assertFalse(iterator.hasNext());
    }
  }

  protected abstract MessageTable getTable() throws Exception;
}
