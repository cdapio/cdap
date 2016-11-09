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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.MetadataTableTest;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * Tests for {@link HBaseMetadataTable}
 */
public class HBaseMetadataTableTest extends MetadataTableTest {

  @ClassRule
  public static final HBaseTestBase TEST_BASE = new HBase98Test();
  private static final CConfiguration cConf = CConfiguration.create();

  private static HBaseTableUtil tableUtil;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    tableUtil = new HBase98TableUtil();
    tableUtil.setCConf(cConf);
    tableUtil.createNamespaceIfNotExists(TEST_BASE.getHBaseAdmin(), tableUtil.getHBaseNamespace(NamespaceId.CDAP));
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    tableUtil.deleteAllInNamespace(TEST_BASE.getHBaseAdmin(), tableUtil.getHBaseNamespace(NamespaceId.CDAP));
    tableUtil.deleteNamespaceIfExists(TEST_BASE.getHBaseAdmin(), tableUtil.getHBaseNamespace(NamespaceId.CDAP));
  }

  @Override
  protected MetadataTable getTable() throws Exception {
    return new HBaseMetadataTable(TEST_BASE.getConfiguration(), tableUtil, "metadata");
  }
}
