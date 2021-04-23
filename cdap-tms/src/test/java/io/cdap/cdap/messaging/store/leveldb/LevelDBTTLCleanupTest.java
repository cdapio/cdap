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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.store.DataCleanupTest;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

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
}
