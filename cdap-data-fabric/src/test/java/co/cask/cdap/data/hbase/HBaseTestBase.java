/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.utils.Networks;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A base class that can be used to easily run a test within an embedded
 * HBase cluster (includes embedded ZooKeeper, HDFS, and HBase).
 *
 * To use, simply extend this class and create your tests like normal.  From
 * within your tests, you can access the underlying HBase cluster through
 * {@link #getConfiguration()}, {@link #getHBaseAdmin()}
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

  public String getZkConnectionString() {
    return "localhost:" + getZKClientPort();
  }

  public abstract int getZKClientPort();

  // Test startup / teardown

  public final void startHBase() throws Exception {
    // Tune down the connection thread pool size
    getConfiguration().setInt("hbase.hconnection.threads.core", 5);
    getConfiguration().setInt("hbase.hconnection.threads.max", 10);
    // Tunn down handler threads in regionserver
    getConfiguration().setInt("hbase.regionserver.handler.count", 10);

    // Set to random port
    getConfiguration().setInt("hbase.master.port", Networks.getRandomPort());
    getConfiguration().setInt("hbase.master.info.port", Networks.getRandomPort());
    getConfiguration().setInt("hbase.regionserver.port", Networks.getRandomPort());
    getConfiguration().setInt("hbase.regionserver.info.port", Networks.getRandomPort());

    doStartHBase();
  }

  public abstract void doStartHBase() throws Exception;

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
  public void forceRegionFlush(byte[] tableName) throws IOException {
    MiniHBaseCluster hbaseCluster = getHBaseCluster();
    if (hbaseCluster != null) {
      TableName qualifiedTableName = TableName.valueOf(tableName);
      for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
        List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(qualifiedTableName);
        List<Runnable> flushers = new ArrayList<>();
        for (HRegion region : serverRegions) {
          flushers.add(createFlushRegion(region));
        }
        parallelRun(flushers);

        LOG.info("RegionServer {}: Flushed {} regions for table {}", t.getRegionServer().getServerName().toString(),
                 serverRegions.size(), Bytes.toStringBinary(tableName));
      }
    }
  }

  /**
   * Force and block on a compaction on all regions of table {@code tableName}.
   * @param tableName The table whose regions should be compacted.
   * @param majorCompact Whether a major compaction should be requested.
   */
  public void forceRegionCompact(byte[] tableName, boolean majorCompact) throws IOException {
    MiniHBaseCluster hbaseCluster = getHBaseCluster();
    if (hbaseCluster != null) {
      TableName qualifiedTableName = TableName.valueOf(tableName);
      for (JVMClusterUtil.RegionServerThread t : hbaseCluster.getRegionServerThreads()) {
        List<HRegion> serverRegions = t.getRegionServer().getOnlineRegions(qualifiedTableName);
        List<Runnable> compacters = new ArrayList<>();
        for (HRegion region : serverRegions) {
          compacters.add(createCompactRegion(region, majorCompact));
        }
        parallelRun(compacters);

        LOG.info("RegionServer {}: Compacted {} regions for table {}", t.getRegionServer().getServerName().toString(),
                 serverRegions.size(), Bytes.toStringBinary(tableName));
      }
    }
  }

  /**
   * Creates a {@link Runnable} that flushes the given HRegion when run.
   */
  public Runnable createFlushRegion(final HRegion region) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          region.flushcache();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * Creates a {@link Runnable} that compacts the given HRegion when run.
   */
  public Runnable createCompactRegion(final HRegion region, final boolean majorCompact) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          region.compactStores(majorCompact);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

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

  /**
   * Executes the given list of Runnable in parallel using a fixed thread pool executor. This method blocks
   * until all runnables finished.
   */
  private void parallelRun(List<? extends Runnable> runnables) {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
      Executors.newFixedThreadPool(runnables.size()));

    try {
      List<ListenableFuture<?>> futures = new ArrayList<>(runnables.size());
      for (Runnable r : runnables) {
        futures.add(executor.submit(r));
      }
      Futures.getUnchecked(Futures.allAsList(futures));
    } finally {
      executor.shutdownNow();
      try {
        executor.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("Interrupted", e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    HBaseTestBase tester = new HBaseTestFactory().get();
    tester.startHBase();
  }
}
