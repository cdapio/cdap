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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * An abstraction on persistent storage of consumer information.
 */
public class MetricsConsumerMetaTable {
  private static final byte[] OFFSET_COLUMN = Bytes.toBytes("o");
  private static final byte[] MESSAGE_ID_COLUMN = Bytes.toBytes("m");

  private static final byte[] PROCESS_COUNT = Bytes.toBytes("pct");
  private static final byte[] PROCESS_TIMESTAMP_OLDEST = Bytes.toBytes("pto");
  private static final byte[] LAST_PROCESS_TIMESTAMP = Bytes.toBytes("lpt");
  private static final byte[] PROCESS_TIMESTAMP_LATEST = Bytes.toBytes("ptl");

  private final MetricsTable metaTable;

  public MetricsConsumerMetaTable(MetricsTable metaTable) {
    this.metaTable = metaTable;
  }

  public synchronized <T extends MetricsMetaKey> void save(Map<T, Long> offsets) throws Exception {
    SortedMap<byte[], SortedMap<byte[], Long>> updates = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<T, Long> entry : offsets.entrySet()) {
      SortedMap<byte[], Long> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      map.put(OFFSET_COLUMN, entry.getValue());
      updates.put(entry.getKey().getKey(), map);
    }
    metaTable.put(updates);
  }

  public <T extends MetricsMetaKey>
  void saveMetricsProcessorStats(Map<T, TopicProcessMeta> messageIds) throws Exception {
    SortedMap<byte[], SortedMap<byte[], byte[]>> updates = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<T, TopicProcessMeta> entry : messageIds.entrySet()) {
      TopicProcessMeta metaInfo = entry.getValue();
      if (metaInfo.getMessagesProcessed() > 0L) {
        SortedMap<byte[], byte[]> columns = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        columns.put(PROCESS_COUNT, Bytes.toBytes(metaInfo.getMessagesProcessed()));
        columns.put(PROCESS_TIMESTAMP_LATEST, Bytes.toBytes(metaInfo.getLatestMetricsTimestamp()));
        columns.put(PROCESS_TIMESTAMP_OLDEST, Bytes.toBytes(metaInfo.getOldestMetricsTimestamp()));
        columns.put(LAST_PROCESS_TIMESTAMP, Bytes.toBytes(metaInfo.getLastProcessedTimestamp()));

        columns.put(MESSAGE_ID_COLUMN, entry.getValue().getMessageId());
        updates.put(entry.getKey().getKey(), columns);
      }
    }
    metaTable.putBytes(updates);
  }

  /**
   * Gets the value as a long in the {@link MetricsTable} of a given key.
   *
   * @param metaKey Object form of the key to get value with.
   * @return The value or {@code -1} if the value is not found.
   * @throws Exception If there is an error when fetching.
   */
  public synchronized <T extends MetricsMetaKey> long get(T metaKey) throws Exception {
    byte[] result = metaTable.get(metaKey.getKey(), OFFSET_COLUMN);
    if (result == null) {
      return -1;
    }
    return Bytes.toLong(result);
  }

  private synchronized long getLong(byte[] rowKey, byte[] column) {
    byte[] result = metaTable.get(rowKey, column);
    if (result == null) {
      return 0;
    }
    return Bytes.toLong(result);
  }

  /**
   * Gets the value as a byte array in the {@link MetricsTable} of a given key.
   *
   * @param metaKey Object form of the key to get value with.
   * @return The value or {@code null} if the value is not found.
   * @throws Exception If there is an error when fetching.
   */
  @Nullable
  public synchronized <T extends MetricsMetaKey> TopicProcessMeta getTopicProcessMeta(T metaKey) throws Exception {
    // TODO : update to use get with multiple columns after CDAP-12459 is fixed
    byte[] messageId = metaTable.get(metaKey.getKey(), MESSAGE_ID_COLUMN);
    if (messageId == null) {
      return null;
    }
    long processedCount = getLong(metaKey.getKey(), PROCESS_COUNT);
    long oldestTs = getLong(metaKey.getKey(), PROCESS_TIMESTAMP_OLDEST);
    long latestTs = getLong(metaKey.getKey(), PROCESS_TIMESTAMP_LATEST);
    long lastProcessedTs = getLong(metaKey.getKey(), LAST_PROCESS_TIMESTAMP);
    return new TopicProcessMeta(messageId, oldestTs, latestTs, processedCount, lastProcessedTs);
  }
}
