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
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.data2.StatusCode;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.common.collect.Maps;
import org.apache.twill.kafka.client.TopicPartition;

import java.util.Collections;
import java.util.Map;

/**
 * An abstraction on persistent storage of kafka consumer information.
 */
public final class KafkaConsumerMetaTable {

  private static final byte[] OFFSET_COLUMN = Bytes.toBytes("o");

  private final MetricsTable metaTable;

  public KafkaConsumerMetaTable(MetricsTable metaTable) {
    this.metaTable = metaTable;
  }

  public synchronized void save(Map<TopicPartition, Long> offsets) throws OperationException {

    Map<byte[], Map<byte[], byte[]>> updates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      updates.put(getKey(entry.getKey()), Collections.singletonMap(OFFSET_COLUMN, Bytes.toBytes(entry.getValue())));
    }
    try {
      metaTable.put(updates);
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Gets the offset of a given topic partition.
   * @param topicPartition The topic and partition to fetch offset.
   * @return The offset or {@code -1} if the offset is not found.
   * @throws OperationException If there is an error when fetching.
   */
  public synchronized long get(TopicPartition topicPartition) throws OperationException {
    try {
      byte[] result = metaTable.get(getKey(topicPartition), OFFSET_COLUMN);
      if (result == null) {
        return -1;
      }
      return Bytes.toLong(result);
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  private byte[] getKey(TopicPartition topicPartition) {
    return Bytes.toBytes(String.format("%s.%02d", topicPartition.getTopic(), topicPartition.getPartition()));
  }
}
