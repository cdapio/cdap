package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A table of {@link TTQueue}s.  See that API for details.
 */
public class TTQueueTableOnHBaseNative implements TTQueueTable {

  private final HTable table;
  private final TransactionOracle oracle;
  private final Configuration hbaseConf;
  private final CConfiguration conf;

  private final ConcurrentSkipListMap<byte[], TTQueue> queues =
      new ConcurrentSkipListMap<byte[],TTQueue>(Bytes.BYTES_COMPARATOR);

  public TTQueueTableOnHBaseNative(HTable table, TransactionOracle oracle,
                                   CConfiguration conf, Configuration hbaseConf) {
    this.table = table;
    this.oracle = oracle;
    this.conf = conf;
    this.hbaseConf = hbaseConf;
  }

  private TTQueue getQueue(byte [] queueName) {
    TTQueue queue = this.queues.get(queueName);
    if (queue != null) return queue;
    queue = new TTQueueOnHBaseNative(this.table, queueName, this.oracle,
        this.conf);
    TTQueue existing = this.queues.putIfAbsent(queueName, queue);
    return existing != null ? existing : queue;
  }

  @Override
  public EnqueueResult enqueue(byte[] queueName, QueueEntry entry, Transaction transaction)
    throws OperationException {
    return getQueue(queueName).enqueue(entry, transaction);
  }

  @Override
  public EnqueueResult enqueue(byte[] queueName, QueueEntry[] entries, Transaction transaction)
    throws OperationException {
    return getQueue(queueName).enqueue(entries, transaction);
  }

  @Override
  public void invalidate(byte[] queueName, QueueEntryPointer[] entryPointers, Transaction transaction)
    throws OperationException {
    getQueue(queueName).invalidate(entryPointers, transaction);
  }

  @Override
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer, ReadPointer readPointer)
    throws  OperationException {
    return getQueue(queueName).dequeue(consumer, readPointer);
  }

  @Override
  public void ack(byte[] queueName, QueueEntryPointer entryPointer, QueueConsumer consumer, Transaction transaction)
    throws OperationException {
    getQueue(queueName).ack(entryPointer, consumer, transaction);
  }

  @Override
  public void ack(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer, Transaction transaction)
    throws OperationException {
    getQueue(queueName).ack(entryPointers, consumer, transaction);
  }

  @Override
  public void finalize(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer, int totalNumGroups,
                       Transaction transaction) throws OperationException {
    getQueue(queueName).finalize(entryPointers, consumer, totalNumGroups, transaction);
  }

  @Override
  public void unack(byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer,
                    Transaction transaction) throws OperationException {
    getQueue(queueName).unack(entryPointers, consumer, transaction);
  }

  @Override
  public int configure(byte[] queueName, QueueConsumer newConsumer, ReadPointer readPointer)
    throws OperationException {
    return getQueue(queueName).configure(newConsumer, readPointer);
  }

  @Override
  public List<Long> configureGroups(byte[] queueName, List<Long> groupIds) throws OperationException {
    return getQueue(queueName).configureGroups(groupIds);
  }

  @Override
  public void dropInflightState(byte[] queueName, QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    getQueue(queueName).dropInflightState(consumer, readPointer);
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
      HBaseAdmin hba = new HBaseAdmin(this.hbaseConf);
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
