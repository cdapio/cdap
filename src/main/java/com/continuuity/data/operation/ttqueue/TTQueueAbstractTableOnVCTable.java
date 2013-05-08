package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A table of {@link com.continuuity.data.operation.ttqueue.TTQueue}s.  See that API for details.
 */
public abstract class TTQueueAbstractTableOnVCTable implements TTQueueTable {

  protected final TransactionOracle oracle;
  protected final CConfiguration conf;

  protected final ConcurrentSkipListMap<byte[], TTQueue> queues =
      new ConcurrentSkipListMap<byte[],TTQueue>(Bytes.BYTES_COMPARATOR);

  public TTQueueAbstractTableOnVCTable(TransactionOracle oracle, CConfiguration conf) {
    this.oracle = oracle;
    this.conf = conf;
  }

  protected abstract TTQueue getQueue(byte [] queueName);

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
                               throws OperationException {
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
}
