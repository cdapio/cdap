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

package io.cdap.cdap.data2.dataset2.lib.table.leveldb;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.util.TableId;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import org.iq80.leveldb.DB;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LevelDBTableService#clearTables()} quiet close and {@link TableId}.
 */
public class LevelDBTableServiceClearTablesTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  @SuppressWarnings("unchecked")
  public void testClearTablesClosesAllEntriesQuietlyEvenWhenDbCloseThrowsIoException()
      throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_DATA_LEVELDB_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    try (LevelDBTableService service = new LevelDBTableService()) {
      service.setConfiguration(cConf);
      service.ensureTableExists("default.realTable");

      Field tablesField = LevelDBTableService.class.getDeclaredField("tables");
      tablesField.setAccessible(true);
      ConcurrentMap<String, DB> tablesMap = (ConcurrentMap<String, DB>) tablesField.get(service);

      DB throwingDb1 = Mockito.mock(DB.class);
      Mockito.doThrow(new IOException("db1 close failure")).when(throwingDb1).close();
      DB healthyMockDb2 = Mockito.mock(DB.class);

      tablesMap.put("ns1.throwing1", throwingDb1);
      tablesMap.put("ns1.healthy2", healthyMockDb2);
      Assert.assertEquals(3, tablesMap.size());

      service.clearTables();

      Assert.assertTrue(tablesMap.isEmpty());
      Mockito.verify(throwingDb1).close();
      Mockito.verify(healthyMockDb2).close();
    }
  }

  @Test
  public void testTableIdToStringEqualsAndHashCode() {
    TableId id1 = TableId.from("myNs", "myTable");
    TableId id2 = TableId.from("myNs", "myTable");
    TableId id3 = TableId.from("otherNs", "myTable");

    Assert.assertEquals("TableId{namespace=myNs, tableName=myTable}", id1.toString());
    Assert.assertEquals(id1, id2);
    Assert.assertEquals(id1.hashCode(), id2.hashCode());
    Assert.assertNotEquals(id1, id3);
  }
}
