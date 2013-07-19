/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.process;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import com.continuuity.kafka.client.TopicPartition;

import java.util.Map;

/**
 * An abstraction on persistent storage of kafka consumer information.
 */
public final class KafkaConsumerMetaTable {

  private static final byte[] OFFSET_COLUMN = Bytes.toBytes("o");

  private final VersionedColumnarTable metaTable;

  public KafkaConsumerMetaTable(VersionedColumnarTable metaTable) {
    this.metaTable = metaTable;
  }

  public void save(Map<TopicPartition, Long> offsets) throws OperationException {
    byte[][] rows = new byte[offsets.size()][];
    byte[][] cols = new byte[offsets.size()][];
    byte[][] vals = new byte[offsets.size()][];

    int idx = 0;
    for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
      rows[idx] = getKey(entry.getKey());
      cols[idx] = OFFSET_COLUMN;
      vals[idx] = Bytes.toBytes(entry.getValue());
    }

    metaTable.put(rows, cols, System.currentTimeMillis(), vals);
  }

  /**
   * Gets the offset of a given topic partition.
   * @param topicPartition The topic and partition to fetch offset.
   * @return The offset or {@code -1} if the offset is not found.
   * @throws OperationException If there is an error when fetching.
   */
  public long get(TopicPartition topicPartition) throws OperationException {
    OperationResult<byte[]> result = metaTable.get(getKey(topicPartition), OFFSET_COLUMN, MemoryReadPointer.DIRTY_READ);
    if (result == null || result.isEmpty()) {
      return -1;
    }
    return Bytes.toLong(result.getValue());
  }

  private byte[] getKey(TopicPartition topicPartition) {
    return Bytes.toBytes(String.format("%s.%02d", topicPartition.getTopic(), topicPartition.getPartition()));
  }
}
