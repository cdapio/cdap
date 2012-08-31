package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.ReadPointer;
import com.continuuity.data.table.VersionedColumnarTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnVCTable implements TTQueueTable {

  private final VersionedColumnarTable table;
  private final TimestampOracle timeOracle;
  private final Configuration conf;

  private final ConcurrentSkipListMap<byte[], TTQueue> queues =
      new ConcurrentSkipListMap<byte[],TTQueue>(Bytes.BYTES_COMPARATOR);

  public TTQueueTableOnVCTable(VersionedColumnarTable table,
      TimestampOracle timeOracle, Configuration conf) {
    this.table = table;
    this.timeOracle = timeOracle;
    this.conf = conf;
  }

  private TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueOnVCTable(this.table, queueName, this.timeOracle,
        this.conf);
    TTQueue existing = this.queues.putIfAbsent(queueName, queue);
    return existing != null ? existing : queue;
  }

  @Override
  public EnqueueResult enqueue(byte [] queueName, byte [] data,
      long writeVersion) throws OperationException {
    return getQueue(queueName).enqueue(data, writeVersion);
  }

  @Override
  public void invalidate(byte [] queueName, QueueEntryPointer entryPointer,
      long writeVersion) {
    getQueue(queueName).invalidate(entryPointer, writeVersion);
  }

  @Override
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, ReadPointer readPointer) throws OperationException {
    return getQueue(queueName).dequeue(consumer, config, readPointer);
  }

  @Override
  public void ack(byte[] queueName, QueueEntryPointer entryPointer,
                  QueueConsumer consumer) throws OperationException {
    getQueue(queueName).ack(entryPointer, consumer);
  }

  @Override
  public void finalize(byte[] queueName, QueueEntryPointer entryPointer,
                       QueueConsumer consumer, int totalNumGroups) throws OperationException {
    getQueue(queueName).finalize(entryPointer, consumer, totalNumGroups);
  }

  @Override
  public void unack(byte[] queueName, QueueEntryPointer entryPointer,
                    QueueConsumer consumer) throws OperationException {
    getQueue(queueName).unack(entryPointer, consumer);
  }

  @Override
  public String getGroupInfo(byte[] queueName, int groupId) {
    TTQueue queue = getQueue(queueName);
    if (queue instanceof TTQueueOnVCTable)
      return ((TTQueueOnVCTable)queue).getInfo(groupId);
    return "GroupInfo not supported";
  }

  @Override
  public String getEntryInfo(byte[] queueName, long entryId) {
    TTQueue queue = getQueue(queueName);
    if (queue instanceof TTQueueOnVCTable)
      return ((TTQueueOnVCTable)queue).getEntryInfo(entryId);
    return "EntryInfo not supported";
  }

  @Override
  public long getGroupID(byte[] queueName) throws OperationException {
    return getQueue(queueName).getGroupID();
  }

  @Override
  public QueueMeta getQueueMeta(byte[] queueName) {
    return getQueue(queueName).getQueueMeta();
  }

  @Override
  public void clear() throws OperationException {
    table.clear();
  }
}
