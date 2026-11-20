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

package io.cdap.cdap.messaging.store.leveldb;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.PayloadTable;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.iq80.leveldb.DB;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LevelDBTableFactory} closeQuietly and old-generation cleanup paths.
 */
public class LevelDBTableFactoryTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCloseSwallowsDbCloseExceptionsAndHandlesNullMetadataTable() throws Exception {
    File baseDir = TEMP_FOLDER.newFolder();
    CConfiguration cConf = createConfig(baseDir);
    LevelDBTableFactory factory = new LevelDBTableFactory(cConf);
    factory.init();

    // Closing before any tables are opened (metadataTable == null) must succeed cleanly
    factory.close();

    // Re-open metadata, message, and payload tables, plus inject a DB that throws on close()
    TopicId topicId = NamespaceId.DEFAULT.topic("closeTest");
    TopicMetadata metadata = new DefaultTopicMetadata(topicId, "ttl", "3600", "generation", "1");
    try (MetadataTable metaTable = factory.createMetadataTable();
        MessageTable msgTable = factory.createMessageTable(metadata);
        PayloadTable payloadTable = factory.createPayloadTable(metadata)) {
      Assert.assertNotNull(metaTable);
      Assert.assertNotNull(msgTable);
      Assert.assertNotNull(payloadTable);
    }

    final AtomicBoolean failingDbClosed = new AtomicBoolean(false);
    DB failingDb = Mockito.mock(DB.class);
    Mockito.doAnswer(inv -> {
      failingDbClosed.set(true);
      throw new IOException("simulated DB close error");
    }).when(failingDb).close();

    Field levelDbsField = LevelDBTableFactory.class.getDeclaredField("levelDBs");
    levelDbsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentMap<File, DB> levelDbs = (ConcurrentMap<File, DB>) levelDbsField.get(factory);
    levelDbs.put(new File(baseDir, "synthetic.failing.db"), failingDb);

    factory.close();
    Assert.assertTrue(failingDbClosed.get());
    Assert.assertTrue(levelDbs.isEmpty());
  }

  @Test
  public void testDataCleanupHandlesMissingAndPresentLevelDbMapEntries() throws Exception {
    File baseDir = TEMP_FOLDER.newFolder();
    CConfiguration cConf = createConfig(baseDir);
    LevelDBTableFactory factory = new LevelDBTableFactory(cConf);
    factory.init();

    TopicId topicId = NamespaceId.DEFAULT.topic("cleanupTopic");
    TopicMetadata topicMeta = new DefaultTopicMetadata(topicId, "ttl", "3600");
    try (MetadataTable metadataTable = factory.createMetadataTable()) {
      // Create, delete, and re-create so generation increments from 1 to 2
      metadataTable.createTopic(topicMeta);
      metadataTable.deleteTopic(topicId);
      metadataTable.createTopic(topicMeta);
      Assert.assertEquals(2, metadataTable.getMetadata(topicId).getGeneration());
    }

    // Create generation 1 on-disk directories for message and payload tables:
    // message dir is NOT in levelDBs map (so levelDBs.remove returns null!),
    // payload dir IS in levelDBs map with a DB whose close() throws IOException.
    String msgTableName = cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME);
    String payloadTableName = cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME);
    File gen1MsgDir = new File(baseDir,
        String.format("%s.%s.%s.1", topicId.getNamespace(), msgTableName, topicId.getTopic()));
    File gen1PayloadDir = new File(baseDir,
        String.format("%s.%s.%s.1", topicId.getNamespace(), payloadTableName, topicId.getTopic()));
    Assert.assertTrue(gen1MsgDir.mkdirs());
    Assert.assertTrue(new File(gen1MsgDir, "dummy").createNewFile());
    Assert.assertTrue(gen1PayloadDir.mkdirs());
    Assert.assertTrue(new File(gen1PayloadDir, "dummy").createNewFile());

    final AtomicBoolean payloadDbClosed = new AtomicBoolean(false);
    DB throwingPayloadDb = Mockito.mock(DB.class);
    Mockito.doAnswer(inv -> {
      payloadDbClosed.set(true);
      throw new IOException("close failure during cleanup");
    }).when(throwingPayloadDb).close();

    Field levelDbsField = LevelDBTableFactory.class.getDeclaredField("levelDBs");
    levelDbsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentMap<File, DB> levelDbs = (ConcurrentMap<File, DB>) levelDbsField.get(factory);
    levelDbs.put(gen1PayloadDir, throwingPayloadDb);

    // Invoke DataCleanup.run() synchronously to keep the test deterministic and < 50ms
    Class<?> cleanupClass = Class.forName(LevelDBTableFactory.class.getName() + "$DataCleanup");
    java.lang.reflect.Constructor<?> ctor =
        cleanupClass.getDeclaredConstructor(LevelDBTableFactory.class);
    ctor.setAccessible(true);
    Object cleanupTask = ctor.newInstance(factory);
    Method runMethod = cleanupClass.getDeclaredMethod("run");
    runMethod.setAccessible(true);
    runMethod.invoke(cleanupTask);

    Assert.assertTrue(payloadDbClosed.get());
    Assert.assertFalse(levelDbs.containsKey(gen1PayloadDir));
    Assert.assertFalse(gen1MsgDir.exists());
    Assert.assertFalse(gen1PayloadDir.exists());

    factory.close();
  }

  private static CConfiguration createConfig(File baseDir) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, baseDir.getAbsolutePath());
    cConf.set(Constants.MessagingSystem.LOCAL_DATA_DIR, baseDir.getAbsolutePath());
    // Set large cleanup interval so background thread doesn't race with deterministic test steps
    cConf.set(Constants.MessagingSystem.LOCAL_DATA_CLEANUP_FREQUENCY, "3600");
    return cConf;
  }
}
