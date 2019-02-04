/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
  private static final byte[] MESSAGE_ID_COLUMN = Bytes.toBytes("m");

  private static final byte[] PROCESS_COUNT = Bytes.toBytes("pct");
  private static final byte[] PROCESS_TIMESTAMP_OLDEST = Bytes.toBytes("pto");
  private static final byte[] LAST_PROCESS_TIMESTAMP = Bytes.toBytes("lpt");
  private static final byte[] PROCESS_TIMESTAMP_LATEST = Bytes.toBytes("ptl");

  private final MetricsTable metaTable;

  public MetricsConsumerMetaTable(MetricsTable metaTable) {
    this.metaTable = metaTable;
  }

  public <T extends MetricsMetaKey> void saveMetricsProcessorStats(Map<T, TopicProcessMeta> messageIds) {
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
   * Gets the value as a byte array in the {@link MetricsTable} of a given key.
   *
   * @param metaKey Object form of the key to get value with.
   * @return The value or {@code null} if the value is not found.
   */
  @Nullable
  public synchronized <T extends MetricsMetaKey> TopicProcessMeta getTopicProcessMeta(T metaKey) {
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


  private synchronized long getLong(byte[] rowKey, byte[] column) {
    byte[] result = metaTable.get(rowKey, column);
    if (result == null) {
      return 0L;
    }
    return Bytes.toLong(result);
  }
}
