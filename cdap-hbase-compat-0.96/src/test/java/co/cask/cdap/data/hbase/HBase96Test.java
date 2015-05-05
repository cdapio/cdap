/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.hbase;

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link HBaseTestBase} implementation supporting HBase 0.96.
 */
public class HBase96Test extends HBaseTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(HBase96Test.class);

  private HBaseTestingUtility testUtil = new HBaseTestingUtility();

  @Override
  public Configuration getConfiguration() {
    return testUtil.getConfiguration();
  }

  @Override
  public int getZKClientPort() {
    return testUtil.getZkCluster().getClientPort();
  }

  @Override
  public void startHBase() throws Exception {
    testUtil.startMiniCluster();
  }

  @Override
  public void stopHBase() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Override
  public MiniHBaseCluster getHBaseCluster() {
    return testUtil.getHBaseCluster();
  }

  @Override
  public HRegion createHRegion(byte[] tableName, byte[] startKey,
                               byte[] stopKey, String callingMethod, Configuration conf,
                               byte[]... families)
      throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd.getTableName(), startKey, stopKey, false);
    Path path = new Path(conf.get(HConstants.HBASE_DIR), callingMethod);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    return HRegion.createHRegion(info, path, conf, htd);
  }

  @Override
  public void forceRegionFlush(byte[] tableName) throws IOException {
    MiniHBaseCluster hbaseCluster = getHBaseCluster();
    if (hbaseCluster != null) {
      TableName qualifiedTableName = TableName.valueOf(tableName);
      for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
        List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(qualifiedTableName);
        int cnt = 0;
        for (HRegion region : serverRegions) {
          region.flushcache();
          cnt++;
        }
        LOG.info("RegionServer {}: Flushed {} regions for table {}", t.getRegionServer().getServerName().toString(),
                 cnt, Bytes.toStringBinary(tableName));
      }
    }
  }

  @Override
  public void forceRegionCompact(byte[] tableName, boolean majorCompact) throws IOException {
    MiniHBaseCluster hbaseCluster = getHBaseCluster();
    if (hbaseCluster != null) {
      TableName qualifiedTableName = TableName.valueOf(tableName);
      for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
        List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(qualifiedTableName);
        int cnt = 0;
        for (HRegion region : serverRegions) {
          region.compactStores(majorCompact);
          cnt++;
        }
        LOG.info("RegionServer {}: Compacted {} regions for table {}", t.getRegionServer().getServerName().toString(),
                 cnt, Bytes.toStringBinary(tableName));
      }
    }
  }

  @Override
  public <T> Map<byte[], T> forEachRegion(byte[] tableName, Function<HRegion, T> function) {
    MiniHBaseCluster hbaseCluster = getHBaseCluster();
    Map<byte[], T> results = new TreeMap<byte[], T>(Bytes.BYTES_COMPARATOR);
    // make sure consumer config cache is updated
    for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
      List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(TableName.valueOf(tableName));
      for (HRegion region : serverRegions) {
        results.put(region.getRegionName(), function.apply(region));
      }
    }
    return results;
  }

  @Override
  public void waitUntilTableAvailable(byte[] tableName, long timeoutInMillis)
      throws IOException, InterruptedException {
    testUtil.waitTableAvailable(tableName, timeoutInMillis);
    testUtil.waitUntilAllRegionsAssigned(TableName.valueOf(tableName), timeoutInMillis);
  }
}
