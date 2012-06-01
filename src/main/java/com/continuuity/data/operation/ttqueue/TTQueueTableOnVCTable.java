package com.continuuity.data.operation.ttqueue;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnVCTable implements TTQueueTable {

  private VersionedColumnarTable table;
  private TimestampOracle timeOracle;
  private Configuration conf;
  
  private final ConcurrentSkipListMap<byte[], TTQueueOnVCTable> queues =
      new ConcurrentSkipListMap<byte[],TTQueueOnVCTable>(
          Bytes.BYTES_COMPARATOR);

  public TTQueueTableOnVCTable(VersionedColumnarTable table,
      TimestampOracle timeOracle, Configuration conf) {
    this.table = table;
    this.timeOracle = timeOracle;
    this.conf = conf;
  }
  
  private TTQueueOnVCTable getQueue(byte [] queueName) {
    TTQueueOnVCTable queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueOnVCTable(table, queueName, timeOracle, conf);
    TTQueueOnVCTable existing = this.queues.putIfAbsent(queueName, queue);
    return existing != null ? existing : queue;
  }
  
  @Override
  public EnqueueResult enqueue(byte [] queueName, byte [] data,
      long writeVersion) {
    return getQueue(queueName).enqueue(data, writeVersion);
  }

  @Override
  public void invalidate(byte [] queueName, QueueEntryPointer entryPointer,
      long writeVersion) {
    getQueue(queueName).invalidate(entryPointer, writeVersion);
  }

  @Override
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, ReadPointer readPointer) {
    return getQueue(queueName).dequeue(consumer, config, readPointer);
  }

  @Override
  public boolean ack(byte [] queueName, QueueEntryPointer entryPointer,
      QueueConsumer consumer) {
    return getQueue(queueName).ack(entryPointer, consumer);
  }
}
