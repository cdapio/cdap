package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.table.ReadPointer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnHBaseNative implements TTQueueTable {

  private final HTable table;
  private final TimestampOracle timeOracle;
  private final Configuration conf;

  private final ConcurrentSkipListMap<byte[], TTQueue> queues =
      new ConcurrentSkipListMap<byte[],TTQueue>(Bytes.BYTES_COMPARATOR);

  public TTQueueTableOnHBaseNative(HTable table,
      TimestampOracle timeOracle, Configuration conf) {
    this.table = table;
    this.timeOracle = timeOracle;
    this.conf = conf;
  }

  private TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueOnHBaseNative(this.table, queueName, this.timeOracle,
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
      long writeVersion) throws OperationException {
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
  public String getGroupInfo(byte[] queueName, int groupId)
      throws OperationException {
    TTQueue queue = getQueue(queueName);
    if (queue instanceof TTQueueOnVCTable)
      return ((TTQueueOnVCTable)queue).getInfo(groupId);
    return "GroupInfo not supported";
  }

  @Override
  public String getEntryInfo(byte[] queueName, long entryId)
      throws OperationException {
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
  public QueueInfo getQueueInfo(byte[] queueName) throws OperationException {
    return getQueue(queueName).getQueueInfo();
  }

  @Override
  public void clear() throws OperationException {
    try {
      HBaseAdmin hba = new HBaseAdmin(this.conf);
      HTableDescriptor htd = hba.getTableDescriptor(this.table.getTableName());
      hba.disableTable(this.table.getTableName());
      hba.deleteTable(this.table.getTableName());
      hba.createTable(htd);
    } catch (IOException e) {
      e.printStackTrace();
      throw new OperationException(StatusCode.HBASE_ERROR, "Problem clearing");
    }
  }
}
