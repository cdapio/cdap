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

import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A base class that can be used to easily run a test within an embedded
 * HBase cluster (includes embedded ZooKeeper, HDFS, and HBase).
 *
 * To use, simply extend this class and create your tests like normal.  From
 * within your tests, you can access the underlying HBase cluster through
 * {@link #getConfiguration()}, {@link #getHBaseAdmin()}, and
 * {@link #getHTable(byte[])}.
 *
 * Alternatively, you can call the {@link #startHBase()} and {@link #stopHBase()}
 * methods directly from within your own BeforeClass/AfterClass methods.
 *
 * Note:  This test is somewhat heavy-weight and takes 10-20 seconds to startup.
 */
public abstract class HBaseTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTestBase.class);

  // Accessors for test implementations

  public abstract Configuration getConfiguration();

  public HBaseAdmin getHBaseAdmin() throws IOException {
    return new HBaseAdmin(getConfiguration());
  }

  public HTable getHTable(byte [] tableName) throws IOException {
    return new HTable(getConfiguration(), tableName);
  }

  // Temporary directories

  public String getZkConnectionString() {
    return "localhost:" + getZKClientPort();
  }

  public abstract int getZKClientPort();

  // Test startup / teardown

  public abstract void startHBase() throws Exception;

  public abstract void stopHBase() throws Exception;

  // HRegion-level testing

  public abstract HRegion createHRegion(byte[] tableName, byte[] startKey,
                               byte[] stopKey, String callingMethod, Configuration conf,
                               byte[]... families)
    throws IOException;

  /**
   * Force and block on a flush to occur on all regions of table {@code tableName}.
   * @param tableName The table whose regions should be flushed.
   */
  public abstract void forceRegionFlush(byte[] tableName) throws IOException;

  /**
   * Force and block on a compaction on all regions of table {@code tableName}.
   * @param tableName The table whose regions should be compacted.
   * @param majorCompact Whether a major compaction should be requested.
   */
  public abstract void forceRegionCompact(byte[] tableName, boolean majorCompact) throws IOException;


  /**
   * Applies a {@link Function} on each HRegion for a given table, and returns a map of the results, keyed
   * by region name.
   * @param tableName The table whose regions should be processed.
   * @param function The function to apply on each region.
   * @param <T> The return type for the function.
   * @return
   */
  public abstract <T> Map<byte[], T> forEachRegion(byte[] tableName, Function<HRegion, T> function);

  public abstract MiniHBaseCluster getHBaseCluster();

  public abstract void waitUntilTableAvailable(byte[] tableName, long timeoutInMillis)
      throws IOException, InterruptedException;

  public static void main(String[] args) throws Exception {
    HBaseTestBase tester = new HBaseTestFactory().get();
    tester.startHBase();
  }
}
