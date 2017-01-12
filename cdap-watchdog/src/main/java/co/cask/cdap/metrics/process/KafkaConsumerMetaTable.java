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
package co.cask.cdap.metrics.process;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.logging.save.Checkpoint;
import com.google.common.collect.Maps;
import org.apache.twill.kafka.client.TopicPartition;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An abstraction on persistent storage of kafka consumer information.
 */
public final class KafkaConsumerMetaTable {

  private static final byte[] OFFSET_COLUMN = Bytes.toBytes("o");
  private static final byte[] TIMESTAMP_COLUMN = Bytes.toBytes("t");

  private final MetricsTable metaTable;

  public KafkaConsumerMetaTable(MetricsTable metaTable) {
    this.metaTable = metaTable;
  }

  public synchronized void save(Map<TopicPartition, Checkpoint> offsets) throws Exception {

    SortedMap<byte[], SortedMap<byte[], Long>> updates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<TopicPartition, Checkpoint> entry : offsets.entrySet()) {
      SortedMap<byte[], Long> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      map.put(OFFSET_COLUMN, entry.getValue().getNextOffset());
      map.put(TIMESTAMP_COLUMN, entry.getValue().getMaxEventTime());
      updates.put(getKey(entry.getKey()), map);
    }
    metaTable.put(updates);
  }

  /**
   * Gets the offset and timestamp of a given topic partition.
   * @param topicPartition The topic and partition to fetch offset.
   * @return The {@link Checkpoint} containing the offset ({@code -1} if the offset is not found) and the timestamp
   *         ({@code -2} if the timestamp is not found).
   * @throws Exception If there is an error when fetching.
   */
  public synchronized Checkpoint get(TopicPartition topicPartition) throws Exception {
    byte[] key = getKey(topicPartition);
    byte[] offsetBytes = metaTable.get(key, OFFSET_COLUMN);
    byte[] timeBytes = metaTable.get(key, TIMESTAMP_COLUMN);
    long offset = offsetBytes == null ? -1 : Bytes.toLong(offsetBytes);
    long time = timeBytes == null ? -2 : Bytes.toLong(timeBytes);
    return new Checkpoint(offset, time);
  }

  public synchronized void upgrade() {
    Scanner scanner = metaTable.scan(null, null, null);
    Row rowResult;
    while ((rowResult = scanner.next()) != null) {
      byte[] key = rowResult.getRow();
      byte[] timeBytes = metaTable.get(key, TIMESTAMP_COLUMN);
      if (timeBytes == null) {
        metaTable.swap(key, TIMESTAMP_COLUMN, null, Bytes.toBytes(-2L));
      }
    }
  }

  private byte[] getKey(TopicPartition topicPartition) {
    return Bytes.toBytes(String.format("%s.%02d", topicPartition.getTopic(), topicPartition.getPartition()));
  }
}
