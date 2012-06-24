package com.continuuity.data.operation.ttqueue;

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnVCTable implements TTQueueTable {

  private final VersionedColumnarTable table;
  private final TimestampOracle timeOracle;
  private final Configuration conf;

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
    queue = new TTQueueOnVCTable(this.table, queueName, this.timeOracle, this.conf);
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

  @Override
  public boolean finalize(byte[] queueName, QueueEntryPointer entryPointer,
      QueueConsumer consumer) {
    return getQueue(queueName).finalize(entryPointer, consumer);
  }

  @Override
  public boolean unack(byte[] queueName, QueueEntryPointer entryPointer,
      QueueConsumer consumer) {
    return getQueue(queueName).unack(entryPointer, consumer);
  }

  @Override
  public String getGroupInfo(byte[] queueName, int groupId) {
    return getQueue(queueName).getInfo(groupId);
  }

  @Override
  public String getEntryInfo(byte[] queueName, long entryId) {
    return getQueue(queueName).getEntryInfo(entryId);
  }

  @Override
  public long getGroupID(byte[] queueName) {
    return getQueue(queueName).getGroupID();
  }

  @Override
  public QueueMeta getQueueMeta(byte[] queueName) {
    return getQueue(queueName).getQueueMeta();
  }
}
