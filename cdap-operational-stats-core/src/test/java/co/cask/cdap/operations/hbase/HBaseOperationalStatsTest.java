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

package co.cask.cdap.operations.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.operations.OperationalStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link OperationalStats} for HBase
 */
public class HBaseOperationalStatsTest {
  private static HBaseTestingUtility testHBase;
  private static Configuration conf;

  @BeforeClass
  public static void setup() throws Exception {
    testHBase = new HBaseTestingUtility();
    conf = testHBase.getConfiguration();
    conf.setInt("hbase.hconnection.threads.core", 5);
    conf.setInt("hbase.hconnection.threads.max", 10);
    conf.setInt("hbase.regionserver.handler.count", 10);
    conf.setInt("hbase.master.port", 0);
    conf.setInt("hbase.master.info.port", 0);
    conf.setInt("hbase.regionserver.port", 0);
    conf.setInt("hbase.regionserver.info.port", 0);
    testHBase.startMiniCluster();
    testHBase.createTable(TableName.valueOf(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, "table"),
                          Bytes.toBytes("cf"));
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (testHBase != null) {
      testHBase.shutdownMiniCluster();
    }
  }

  @Test
  public void test() throws IOException {
    HBaseInfo info = new HBaseInfo(conf);
    Assert.assertEquals(AbstractHBaseStats.SERVICE_NAME, info.getServiceName());
    Assert.assertEquals("info", info.getStatType());
    Assert.assertNotNull(info.getVersion());
    String webURL = info.getWebURL();
    String logsURL = info.getLogsURL();
    Assert.assertNull(webURL);
    Assert.assertNull(logsURL);
    info.collect();
    webURL = info.getWebURL();
    logsURL = info.getLogsURL();
    Assert.assertNotNull(webURL);
    Assert.assertNotNull(logsURL);
    Assert.assertEquals(webURL + "/logs", logsURL);
    HBaseNodes nodes = new HBaseNodes(conf);
    Assert.assertEquals("nodes", nodes.getStatType());
    nodes.collect();
    Assert.assertEquals(1, nodes.getMasters());
    Assert.assertEquals(1, nodes.getRegionServers());
    Assert.assertEquals(0, nodes.getDeadRegionServers());
    HBaseLoad load = new HBaseLoad(conf);
    Assert.assertEquals("load", load.getStatType());
    load.collect();
    Assert.assertEquals(load.getTotalRegions(), load.getAverageRegionsPerServer(), 0.0);
    Assert.assertTrue(load.getNumRequests() >= 0);
    Assert.assertTrue(load.getRegionsInTransition() >= 0);
    HBaseEntities entities = new HBaseEntities(conf);
    Assert.assertEquals("entities", entities.getStatType());
    entities.collect();
    // default and system namespaces
    Assert.assertEquals(2, entities.getNamespaces());
    // table created in setup
    Assert.assertEquals(1, entities.getTables());
    Assert.assertEquals(0, entities.getSnapshots());
  }
}
