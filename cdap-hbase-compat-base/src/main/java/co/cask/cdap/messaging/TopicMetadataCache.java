/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.messaging;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.hbase.CConfigurationReader;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Used in coprocessors to get the metadata of a Topic. It also provides the metadata of topics by periodically
 * scanning the Metadata table.
 */
public class TopicMetadataCache extends AbstractIdleService {
  private static final Log LOG = LogFactory.getLog(TopicMetadataCache.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private static final byte[] COL_FAMILY = MessagingUtils.Constants.COLUMN_FAMILY;
  private static final byte[] COL = MessagingUtils.Constants.METADATA_COLUMN;

  private final RegionCoprocessorEnvironment env;
  private final CConfigurationReader cConfReader;
  private final String hbaseNamespacePrefix;
  private final String metadataTableNamespace;
  private final ScanBuilder scanBuilder;

  private volatile Thread refreshThread;
  private volatile boolean stopped;
  private volatile CConfiguration cConf;
  private volatile Map<ByteBuffer, Map<String, String>> metadataCache = new HashMap<>();

  private long lastUpdated;
  private long metadataCacheUpdateFreqInMillis = TimeUnit.SECONDS.toMillis(
    MessagingUtils.Constants.METADATA_CACHE_UPDATE_FREQUENCY_SECS);

  public TopicMetadataCache(RegionCoprocessorEnvironment env, CConfigurationReader cConfReader,
                            String hbaseNamespacePrefix, String metadataTableNamespace, ScanBuilder scanBuilder) {
    this.env = env;
    this.cConfReader = cConfReader;
    this.hbaseNamespacePrefix = hbaseNamespacePrefix;
    this.metadataTableNamespace = metadataTableNamespace;
    this.scanBuilder = scanBuilder;
  }

  public boolean isAlive() {
    return refreshThread.isAlive();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TopicMetadataCache Refresh Thread.");
    startRefreshThread();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping TopicMetadataCache Refresh Thread.");
    stopped = true;
    if (refreshThread != null) {
      refreshThread.interrupt();
      refreshThread.join(TimeUnit.SECONDS.toMillis(1));
    }
  }

  @Nullable
  public Map<String, String> getTopicMetadata(ByteBuffer topicId) {
    return metadataCache.get(topicId);
  }

  @Nullable
  public CConfiguration getCConfiguration() {
    return cConf;
  }

  /**
   * Called in unit tests and since the refresh thread might invoke cache update at the same time, we make this method
   * synchronized. Aside from unit tests, synchronization is not required.
   */
  @VisibleForTesting
  public synchronized void updateCache() throws IOException {
    HTableInterface metadataTable = null;
    long now = System.currentTimeMillis();
    long topicCount = 0;
    try {
      CConfiguration cConf = cConfReader.read();
      if (cConf != null) {
        this.cConf = cConf;
        int metadataScanSize = cConf.getInt(Constants.MessagingSystem.HBASE_SCAN_CACHE_ROWS);
        metadataCacheUpdateFreqInMillis = TimeUnit.SECONDS.toMillis(cConf.getLong(
          Constants.MessagingSystem.COPROCESSOR_METADATA_CACHE_UPDATE_FREQUENCY_SECONDS,
          MessagingUtils.Constants.METADATA_CACHE_UPDATE_FREQUENCY_SECS));
        String tableName = cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME);

        metadataTable = getMetadataTable(tableName);
        if (metadataTable == null) {
          LOG.warn(String.format("Could not find HTableInterface of metadataTable %s:%s. Cannot update metadata cache",
                                 hbaseNamespacePrefix, tableName));
          return;
        }

        Map<ByteBuffer, Map<String, String>> newTopicCache = new HashMap<>();
        Scan scan = scanBuilder.setCaching(metadataScanSize).build();
        try (ResultScanner scanner = metadataTable.getScanner(scan)) {
          for (Result result : scanner) {
            ByteBuffer topicId = ByteBuffer.wrap(result.getRow());
            byte[] value = result.getValue(COL_FAMILY, COL);
            Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
            String ttl = properties.get(MessagingUtils.Constants.TTL_KEY);
            long ttlInMes = TimeUnit.SECONDS.toMillis(Long.parseLong(ttl));
            properties.put(MessagingUtils.Constants.TTL_KEY, Long.toString(ttlInMes));
            newTopicCache.put(topicId, properties);
            topicCount++;
          }
        }

        long elapsed = System.currentTimeMillis()  - now;
        this.metadataCache = newTopicCache;
        this.lastUpdated = now;
        LOG.debug(String.format("Updated consumer config cache with %d topics, took %d msec", topicCount, elapsed));
      }
    } finally {
      if (metadataTable != null) {
        try {
          metadataTable.close();
        } catch (IOException ex) {
          LOG.error("Error closing table. ", ex);
        }
      }
    }
  }

  private HTableInterface getMetadataTable(String tableName) throws IOException {
    return env.getTable(HTableNameConverter.toTableName(hbaseNamespacePrefix,
                                                        TableId.from(metadataTableNamespace, tableName)));
  }

  private void startRefreshThread() {
    refreshThread = new Thread("tms-topic-metadata-cache-refresh") {
      @Override
      public void run() {
        while (!isInterrupted() && !stopped) {
          long now = System.currentTimeMillis();
          if (now > (lastUpdated + metadataCacheUpdateFreqInMillis)) {
            try {
              updateCache();
            } catch (TableNotFoundException ex) {
              LOG.warn("Metadata table not found.", ex);
              break;
            } catch (IOException ex) {
              LOG.warn("Error updating metadata table cache", ex);
            }
          }

          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ex) {
            interrupt();
            break;
          }
        }

        LOG.info("Metadata cache update terminated.");
      }
    };
    refreshThread.setDaemon(true);
    refreshThread.start();
  }
}
