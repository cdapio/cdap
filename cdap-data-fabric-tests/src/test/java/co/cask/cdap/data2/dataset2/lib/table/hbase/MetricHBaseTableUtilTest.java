/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.LocalLocationFactory;
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
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static HBaseTestBase testHBase;
  private static HBaseTableUtil hBaseTableUtil = new HBaseTableUtilFactory().get();

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testHBase.stopHBase();
  }

  @Test
  public void testGetVersion() throws Exception {
    // Verify new metric datasets are properly recognized as 2.8+ version from now on
    HBaseMetricsTableDefinition definition =
      new HBaseMetricsTableDefinition("foo", testHBase.getConfiguration(), hBaseTableUtil,
                                      new LocalLocationFactory(tmpFolder.newFolder()), CConfiguration.create());
    DatasetSpecification spec = definition.configure("metricV2.8", DatasetProperties.EMPTY);

    DatasetAdmin admin = definition.getAdmin(
      new DatasetContext.Builder().setNamespaceId(Constants.SYSTEM_NAMESPACE).build(), spec, null);
    admin.create();

    MetricHBaseTableUtil util = new MetricHBaseTableUtil(hBaseTableUtil);
    HBaseAdmin hAdmin = testHBase.getHBaseAdmin();
    HTableDescriptor desc = hAdmin.getTableDescriptor(Bytes.toBytes(spec.getName()));
    Assert.assertEquals(MetricHBaseTableUtil.Version.VERSION_2_8_OR_HIGHER, util.getVersion(desc));

    // Verify HBase table without coprocessor is properly recognized as 2.6- version
    TableName table26 = TableName.valueOf("metricV2.6");
    hAdmin.createTable(new HTableDescriptor(table26));
    desc = hAdmin.getTableDescriptor(table26);
    Assert.assertEquals(MetricHBaseTableUtil.Version.VERSION_2_6_OR_LOWER, util.getVersion(desc));

    // Verify HBase table with IncrementHandler coprocessor but without cdap.version on it is properly recognized as
    // 2.7 version
    TableName table27 = TableName.valueOf("metricV2.7");
    desc = new HTableDescriptor(table27);
    desc.addCoprocessor(hBaseTableUtil.getIncrementHandlerClassForVersion().getName());
    hAdmin.createTable(desc);
    desc = hAdmin.getTableDescriptor(table27);
    Assert.assertEquals(MetricHBaseTableUtil.Version.VERSION_2_7, util.getVersion(desc));
  }
}
