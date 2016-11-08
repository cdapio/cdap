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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Map;

/**
 * Tests for {@link LevelDBMetadataTable}
 */
public class LevelDBMetadataTableTest {
  private static final String TABLE_NAME = "metadataTable";

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static LevelDBTableService service;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_DATA_LEVELDB_DIR, tmpFolder.newFolder().getAbsolutePath());
    service = new LevelDBTableService();
    service.setConfiguration(conf);
    service.ensureTableExists(TABLE_NAME);
  }

  @Test
  public void testTopicManagement() throws Exception {
    LevelDBMetadataTable table = new LevelDBMetadataTable(service, TABLE_NAME);
    Assert.assertTrue(table.listTopics(NamespaceId.DEFAULT).isEmpty());
    Assert.assertNull(table.getProperties(NamespaceId.DEFAULT.topic("t1")));
    table.createTopic(NamespaceId.DEFAULT.topic("t1"), null);
    Assert.assertEquals(1, table.listTopics(NamespaceId.DEFAULT).size());
    Map<String, String> props = ImmutableMap.of("k1", "v1");
    table.createTopic(NamespaceId.DEFAULT.topic("t2"), props);
    Assert.assertEquals(props, table.getProperties(NamespaceId.DEFAULT.topic("t2")));
    Assert.assertEquals(2, table.listTopics(NamespaceId.DEFAULT).size());
    table.deleteTopic(NamespaceId.DEFAULT.topic("t1"));
    Assert.assertEquals(1, table.listTopics(NamespaceId.DEFAULT).size());
    table.deleteTopic(NamespaceId.DEFAULT.topic("t2"));
    Assert.assertTrue(table.listTopics(NamespaceId.DEFAULT).isEmpty());
  }
}
