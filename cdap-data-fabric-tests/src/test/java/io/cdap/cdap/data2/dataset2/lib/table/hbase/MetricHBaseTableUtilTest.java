/*
 * Copyright 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.hbase;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import io.cdap.cdap.data.hbase.HBaseTestBase;
import io.cdap.cdap.data.hbase.HBaseTestFactory;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class MetricHBaseTableUtilTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static HBaseTableUtil hBaseTableUtil;
  private static CConfiguration cConf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = CConfiguration.create();
    hBaseTableUtil = new HBaseTableUtilFactory(cConf, new SimpleNamespaceQueryAdmin()).get();
    HBaseDDLExecutor executor = new HBaseDDLExecutorFactory(cConf, TEST_HBASE.getHBaseAdmin().getConfiguration()).get();
    executor.createNamespaceIfNotExists(hBaseTableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
  }

  @Test
  public void testGetVersion() throws Exception {
    // Verify new metric datasets are properly recognized as 2.8+ version from now on
    HBaseMetricsTableDefinition definition =
      new HBaseMetricsTableDefinition("foo", TEST_HBASE.getConfiguration(), () -> hBaseTableUtil,
                                      new FileContextLocationFactory(TEST_HBASE.getConfiguration()), cConf);
    DatasetSpecification spec = definition.configure("metricV2.8", DatasetProperties.EMPTY);

    DatasetAdmin admin = definition.getAdmin(DatasetContext.from(NamespaceId.SYSTEM.getNamespace()), spec, null);
    admin.create();

    MetricHBaseTableUtil util = new MetricHBaseTableUtil(hBaseTableUtil);
    HBaseAdmin hAdmin = TEST_HBASE.getHBaseAdmin();
    TableId hTableId = hBaseTableUtil.createHTableId(NamespaceId.SYSTEM, spec.getName());
    HTableDescriptor desc = hBaseTableUtil.getHTableDescriptor(hAdmin, hTableId);
    desc.addFamily(new HColumnDescriptor("c"));
    Assert.assertEquals(MetricHBaseTableUtil.Version.VERSION_2_8_OR_HIGHER, util.getVersion(desc));

    // Verify HBase table without coprocessor is properly recognized as 2.6- version
    TableName table26 = TableName.valueOf("metricV2.6");
    hAdmin.createTable(new HTableDescriptor(table26).addFamily(new HColumnDescriptor("c")));
    desc = hAdmin.getTableDescriptor(table26);
    Assert.assertEquals(MetricHBaseTableUtil.Version.VERSION_2_6_OR_LOWER, util.getVersion(desc));

    // Verify HBase table with IncrementHandler coprocessor but without cdap.version on it is properly recognized as
    // 2.7 version
    TableName table27 = TableName.valueOf("metricV2.7");
    desc = new HTableDescriptor(table27).addFamily(new HColumnDescriptor("c"));
    desc.addCoprocessor(hBaseTableUtil.getIncrementHandlerClassForVersion().getName());
    hAdmin.createTable(desc);
    desc = hAdmin.getTableDescriptor(table27);
    Assert.assertEquals(MetricHBaseTableUtil.Version.VERSION_2_7, util.getVersion(desc));
  }
}
