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
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.messaging.store.MessageTableTest;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * HBase implementation of {@link MessageTable}.
 */
public class HBaseMessageTableTest extends MessageTableTest {

  @ClassRule
  public static final HBaseTestBase TEST_BASE = new HBaseTestFactory().get();
  private static final CConfiguration cConf = CConfiguration.create();

  private static HBaseAdmin hBaseAdmin;
  private static HBaseTableUtil tableUtil;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    hBaseAdmin = TEST_BASE.getHBaseAdmin();
    hBaseAdmin.getConfiguration().set(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION,
                                      HBaseTableUtil.CompressionType.NONE.name());
    tableUtil = new HBaseTableUtilFactory(cConf).get();
    tableUtil.createNamespaceIfNotExists(hBaseAdmin, tableUtil.getHBaseNamespace(NamespaceId.CDAP));
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    tableUtil.deleteAllInNamespace(hBaseAdmin, tableUtil.getHBaseNamespace(NamespaceId.CDAP));
    tableUtil.deleteNamespaceIfExists(hBaseAdmin, tableUtil.getHBaseNamespace(NamespaceId.CDAP));
  }

  @Override
  protected MessageTable getMessageTable() throws Exception {
    byte[] columnFamily = { 'd' };
    TableId tableId = tableUtil.createHTableId(NamespaceId.CDAP, "messageTable");
    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
    HTableDescriptorBuilder htd = tableUtil.buildHTableDescriptor(tableId).addFamily(hcd);
    tableUtil.createTableIfNotExists(hBaseAdmin, tableId, htd.build());
    return new HBaseMessageTable(tableUtil,
                                 tableUtil.createHTable(hBaseAdmin.getConfiguration(), tableId), columnFamily);
  }
}
