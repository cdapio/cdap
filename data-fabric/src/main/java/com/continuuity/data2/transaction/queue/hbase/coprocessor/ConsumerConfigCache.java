package com.continuuity.data2.transaction.queue.hbase.coprocessor;

import com.continuuity.data2.transaction.queue.QueueConstants;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Provides a RegionServer shared cache for all instances of {@link HBaseQueueRegionObserver} of the recent
 * queue consumer configuration.
 */
public class ConsumerConfigCache {
  private static final Log LOG = LogFactory.getLog(ConsumerConfigCache.class);

  private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  // update interval
  private static final long UPDATE_FREQUENCY = 60 * 1000L;

  private static ConcurrentMap<byte[], ConsumerConfigCache> instances =
    new ConcurrentSkipListMap<byte[], ConsumerConfigCache>(Bytes.BYTES_COMPARATOR);

  private static Object lock = new Object();

  private final byte[] configTableName;
  private final Configuration hConf;

  private Thread refreshThread;
  private long lastUpdated;
  private volatile Map<byte[], QueueConsumerConfig> configCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

  ConsumerConfigCache(Configuration hConf, byte[] configTableName) {
    this.hConf = hConf;
    this.configTableName = configTableName;
  }

  private void init() {
    startRefreshThread();
  }

  public QueueConsumerConfig getConsumerConfig(byte[] queueName) {
    QueueConsumerConfig consumerConfig = configCache.get(queueName);
    if (consumerConfig == null) {
      consumerConfig = new QueueConsumerConfig(new HashMap<ConsumerInstance, byte[]>(), 0);
    }
    return consumerConfig;
  }

  private void updateCache() {
    Map<byte[], QueueConsumerConfig> newCache = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    long now = System.currentTimeMillis();
    HTable table = null;
    try {
      table = new HTable(hConf, configTableName);
      Scan scan = new Scan();
      scan.addFamily(QueueConstants.COLUMN_FAMILY);
      ResultScanner scanner = table.getScanner(scan);
      int configCnt = 0;
      for (Result result : scanner) {
        if (!result.isEmpty()) {
          NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(QueueConstants.COLUMN_FAMILY);
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
      LOG.info("Updated consumer config cache with " + configCnt + " entries, took " + elapsed + " msec.");
    } catch (IOException ioe) {
      LOG.warn("Error updating queue consumer config cache: " + ioe.getMessage());
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ioe) {
          LOG.error("Error closing table " + Bytes.toString(configTableName), ioe);
        }
      }
    }

  }

  private void startRefreshThread() {
    this.refreshThread = new Thread() {
      @Override
      public void run() {
        while (!isInterrupted()) {
          long now = System.currentTimeMillis();
          if ((now - lastUpdated) > UPDATE_FREQUENCY) {
            updateCache();
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            // reset status
            interrupt();
            break;
          }
        }
      }
    };
    this.refreshThread.setDaemon(true);
    this.refreshThread.start();
  }

  public static ConsumerConfigCache getInstance(Configuration hConf, byte[] tableName) {
    ConsumerConfigCache cache = instances.get(tableName);
    if (cache == null) {
      cache = new ConsumerConfigCache(hConf, tableName);
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
