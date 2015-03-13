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

package co.cask.cdap.data2.transaction.queue.hbase.coprocessor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueEntryRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;

/**
 * Provides a RegionServer shared cache for all instances of {@code HBaseQueueRegionObserver} of the recent
 * queue consumer configuration.
 */
public class ConsumerConfigCache {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerConfigCache.class);

  private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  // update interval for CConfiguration
  private static final long CONFIG_UPDATE_FREQUENCY = 300 * 1000L;

  private static ConcurrentMap<byte[], ConsumerConfigCache> instances =
    new ConcurrentSkipListMap<byte[], ConsumerConfigCache>(Bytes.BYTES_COMPARATOR);

  private final byte[] queueConfigTableName;
  private final Configuration hConf;
  private final CConfigurationReader cConfReader;

  private Thread refreshThread;
  private long lastUpdated;
  private volatile Map<byte[], QueueConsumerConfig> configCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  private long configCacheUpdateFrequency = QueueConstants.DEFAULT_QUEUE_CONFIG_UPDATE_FREQUENCY;
  private CConfiguration conf;
  // timestamp of the last update from the configuration table
  private long lastConfigUpdate;

  ConsumerConfigCache(Configuration hConf, byte[] queueConfigTableName, CConfigurationReader cConfReader) {
    this.hConf = hConf;
    this.queueConfigTableName = queueConfigTableName;
    this.cConfReader = cConfReader;
  }

  private void init() {
    startRefreshThread();
  }

  public boolean isAlive() {
    return refreshThread.isAlive();
  }

  @Nullable
  public QueueConsumerConfig getConsumerConfig(byte[] queueName) {
    return configCache.get(queueName);
  }

  private void updateConfig() {
    long now = System.currentTimeMillis();
    if (this.conf == null || now > (lastConfigUpdate + CONFIG_UPDATE_FREQUENCY)) {
      try {
        this.conf = cConfReader.read();
        if (this.conf != null) {
          LOG.info("Reloaded CConfiguration at {}", now);
          this.lastConfigUpdate = now;
          long configUpdateFrequency = conf.getLong(QueueConstants.QUEUE_CONFIG_UPDATE_FREQUENCY,
                                                    QueueConstants.DEFAULT_QUEUE_CONFIG_UPDATE_FREQUENCY);
          LOG.info("Will reload consumer config cache every {} seconds", configUpdateFrequency);
          this.configCacheUpdateFrequency = configUpdateFrequency * 1000;
        }
      } catch (IOException ioe) {
        LOG.error("Error reading default configuration table", ioe);
      }
    }
  }

  /**
   * This forces an immediate update of the config cache. It should only be called from the refresh thread or from
   * tests, to avoid having to add a sleep for the duration of the refresh interval.
   *
   * This method is synchronized to protect from race conditions if called directly from a test. Otherwise this is
   * only called from the refresh thread, and there will not be concurrent invocations.
   *
   * @throws IOException if failed to update config cache
   */
  @VisibleForTesting
  public synchronized void updateCache() throws IOException {
    Map<byte[], QueueConsumerConfig> newCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    long now = System.currentTimeMillis();
    HTable table = null;
    try {
      table = new HTable(hConf, queueConfigTableName);
      Scan scan = new Scan();
      scan.addFamily(QueueEntryRow.COLUMN_FAMILY);
      ResultScanner scanner = table.getScanner(scan);
      int configCnt = 0;
      for (Result result : scanner) {
        if (!result.isEmpty()) {
          NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(QueueEntryRow.COLUMN_FAMILY);
          if (familyMap != null) {
            configCnt++;
            Map<ConsumerInstance, byte[]> consumerInstances = new HashMap<ConsumerInstance, byte[]>();
            // Gather the startRow of all instances across all consumer groups.
            int numGroups = 0;
            Long groupId = null;
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
              long gid = Bytes.toLong(entry.getKey());
              int instanceId = Bytes.toInt(entry.getKey(), LONG_BYTES);
              consumerInstances.put(new ConsumerInstance(gid, instanceId), entry.getValue());

              // Columns are sorted by groupId, hence if it change, then numGroups would get +1
              if (groupId == null || groupId != gid) {
                numGroups++;
                groupId = gid;
              }
            }
            byte[] queueName = result.getRow();
            newCache.put(queueName, new QueueConsumerConfig(consumerInstances, numGroups));
          }
        }
      }
      long elapsed = System.currentTimeMillis() - now;
      this.configCache = newCache;
      this.lastUpdated = now;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated consumer config cache with {} entries, took {} msec", configCnt, elapsed);
      }
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.error("Error closing table {}", Bytes.toString(queueConfigTableName), ioe);
        }
      }
    }
  }

  private void startRefreshThread() {
    refreshThread = new Thread("queue-cache-refresh") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          updateConfig();
          long now = System.currentTimeMillis();
          if (now > (lastUpdated + configCacheUpdateFrequency)) {
            try {
              updateCache();
            } catch (TableNotFoundException e) {
              // This is expected when the namespace goes away since there is one config table per namespace
              // If the table is not found due to other situation, the region observer already
              // has logic to get a new one through the getInstance method
              LOG.warn("Queue config table not found: {}", Bytes.toString(queueConfigTableName), e);
              break;
            } catch (IOException e) {
              LOG.warn("Error updating queue consumer config cache", e);
            }
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            // reset status
            interrupt();
            break;
          }
        }
        LOG.info("Config cache update for {} terminated.", Bytes.toString(queueConfigTableName));
        instances.remove(queueConfigTableName, this);
      }
    };
    refreshThread.setDaemon(true);
    refreshThread.start();
  }

  public static ConsumerConfigCache getInstance(Configuration hConf, byte[] tableName,
                                                CConfigurationReader cConfReader) {
    ConsumerConfigCache cache = instances.get(tableName);
    if (cache == null) {
      cache = new ConsumerConfigCache(hConf, tableName, cConfReader);
      if (instances.putIfAbsent(tableName, cache) == null) {
        // if another thread created an instance for the same table, that's ok, we only init the one saved
        cache.init();
      } else {
        // discard our instance and re-retrieve, someone else set it
        cache = instances.get(tableName);
      }
    }
    return cache;
  }
}
