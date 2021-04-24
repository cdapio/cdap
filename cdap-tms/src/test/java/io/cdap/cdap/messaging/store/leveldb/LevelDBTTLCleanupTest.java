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

package io.cdap.cdap.messaging.store.leveldb;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.DataCleanupTest;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for TTL Cleanup logic in LevelDB.
 */
public class LevelDBTTLCleanupTest extends DataCleanupTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static LevelDBTableFactory tableFactory;

  @BeforeClass
  public static void init() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    // Disable the automatic cleanup
    cConf.setInt(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_FREQUENCY, -1);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    // Use a very small batch size
    cConf.setInt(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_BATCH_SIZE, 1);
    tableFactory = new LevelDBTableFactory(cConf);
  }

  @Override
  protected void forceFlushAndCompact(Table table) {
    tableFactory.performCleanup(System.currentTimeMillis());
  }

  @Override
  protected MetadataTable getMetadataTable() throws Exception {
    return tableFactory.createMetadataTable();
  }

  @Override
  protected PayloadTable getPayloadTable(TopicMetadata topicMetadata) throws Exception {
    return tableFactory.createPayloadTable(topicMetadata);
  }

  @Override
  protected MessageTable getMessageTable(TopicMetadata topicMetadata) throws Exception {
    return tableFactory.createMessageTable(topicMetadata);
  }

  @Test
  public void testCleanupBatch() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("cleanup");
    TopicMetadata topic = new TopicMetadata(topicId, "ttl", "3", TopicMetadata.GENERATION_KEY, "1");
    try (MetadataTable metadataTable = getMetadataTable();
         MessageTable messageTable = getMessageTable(topic)) {
      metadataTable.createTopic(topic);
      List<MessageTable.Entry> entries = new ArrayList<>();

      for (int i = 0; i < 100; i++) {
        entries.add(new TestMessageEntry(topicId, 1, 1, "data" + i, 0L, (short) i));
      }
      messageTable.store(entries.iterator());

      // Fetch the entries and make sure we are able to read it
      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        for (int i = 0; i < 100; i++) {
          Assert.assertTrue(iterator.hasNext());
          MessageTable.Entry entry = iterator.next();
          Assert.assertEquals("data" + i, Bytes.toString(entry.getPayload()));
        }
      }

      // Cleanup all messages. We used timestamp = 1 when publishing the data with TTL=3 seconds.
      // So cleanup with 5 sec as current time should have all data removed
      tableFactory.performCleanup(5000);

      try (CloseableIterator<MessageTable.Entry> iterator = messageTable.fetch(topic, 0, Integer.MAX_VALUE, null)) {
        Assert.assertFalse(iterator.hasNext());
      }
    }
  }
}
