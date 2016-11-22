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

package co.cask.cdap.operations.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;

/**
 * Tests for HDFS Operational Stats.
 */
public class HDFSOperationalStatsTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TMP_FOLDER.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(2).build();
    dfsCluster.waitClusterUp();
  }

  @AfterClass
  public static void teardown() {
    dfsCluster.shutdown();
  }

  @Test
  public void test() throws IOException {
    final DistributedFileSystem dfs = dfsCluster.getFileSystem();
    HDFSInfo hdfsInfo = new HDFSInfo(dfs.getConf());
    Assert.assertEquals(AbstractHDFSStats.SERVICE_NAME, hdfsInfo.getServiceName());
    Assert.assertEquals(HDFSInfo.STAT_TYPE, hdfsInfo.getStatType());
    Assert.assertNotNull(hdfsInfo.getVersion());
    String webAddress = hdfsInfo.getWebURL();
    Assert.assertNotNull(webAddress);
    String logsAddress = hdfsInfo.getLogsURL();
    Assert.assertNotNull(logsAddress);
    URL webURL = new URL(webAddress);
    URL logsURL = new URL(logsAddress);
    Assert.assertNotNull(logsURL);
    Assert.assertEquals(webURL.getProtocol(), logsURL.getProtocol());
    Assert.assertEquals(webURL.getHost(), logsURL.getHost());
    Assert.assertEquals(webURL.getPort(), logsURL.getPort());
    Assert.assertEquals("/logs", logsURL.getPath());
    HDFSNodes hdfsNodes = new HDFSNodes(dfs.getConf());
    Assert.assertEquals(AbstractHDFSStats.SERVICE_NAME, hdfsNodes.getServiceName());
    Assert.assertEquals(HDFSNodes.STAT_TYPE, hdfsNodes.getStatType());
    Assert.assertEquals(0, hdfsNodes.getNamenodes());
    hdfsNodes.collect();
    Assert.assertEquals(1, hdfsNodes.getNamenodes());
    HDFSStorage hdfsStorage = new HDFSStorage(dfs.getConf());
    Assert.assertEquals(AbstractHDFSStats.SERVICE_NAME, hdfsStorage.getServiceName());
    Assert.assertEquals(HDFSStorage.STAT_TYPE, hdfsStorage.getStatType());
    Assert.assertEquals(0, hdfsStorage.getTotalBytes());
    Assert.assertEquals(0, hdfsStorage.getUsedBytes());
    Assert.assertEquals(0, hdfsStorage.getRemainingBytes());
    hdfsStorage.collect();
    Assert.assertEquals(0, hdfsStorage.getMissingBlocks());
    Assert.assertEquals(0, hdfsStorage.getUnderReplicatedBlocks());
    Assert.assertEquals(0, hdfsStorage.getCorruptBlocks());
    Assert.assertTrue(hdfsStorage.getTotalBytes() > hdfsStorage.getRemainingBytes());
    Assert.assertTrue(hdfsStorage.getTotalBytes() > hdfsStorage.getUsedBytes());
    Assert.assertTrue(hdfsStorage.getRemainingBytes() > 0);
    Assert.assertTrue(hdfsStorage.getUsedBytes() > 0);
  }
}
