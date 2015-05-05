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
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.common.collect.Maps;
import org.apache.twill.kafka.client.TopicPartition;

import java.util.Map;
import java.util.NavigableMap;

/**
 * An abstraction on persistent storage of kafka consumer information.
 */
public final class KafkaConsumerMetaTable {

  private static final byte[] OFFSET_COLUMN = Bytes.toBytes("o");

  private final MetricsTable metaTable;

  public KafkaConsumerMetaTable(MetricsTable metaTable) {
    this.metaTable = metaTable;
  }

  public synchronized void save(Map<TopicPartition, Long> offsets) throws Exception {

    NavigableMap<byte[], NavigableMap<byte[], Long>> updates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      updates.put(getKey(entry.getKey()), Bytes.immutableSortedMapOf(OFFSET_COLUMN, entry.getValue()));
    }
    metaTable.put(updates);
  }

  /**
   * Gets the offset of a given topic partition.
   * @param topicPartition The topic and partition to fetch offset.
   * @return The offset or {@code -1} if the offset is not found.
   * @throws Exception If there is an error when fetching.
   */
  public synchronized long get(TopicPartition topicPartition) throws Exception {
    byte[] result = metaTable.get(getKey(topicPartition), OFFSET_COLUMN);
    if (result == null) {
      return -1;
    }
    return Bytes.toLong(result);
  }

  private byte[] getKey(TopicPartition topicPartition) {
    return Bytes.toBytes(String.format("%s.%02d", topicPartition.getTopic(), topicPartition.getPartition()));
  }
}
