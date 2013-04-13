package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentSkipListMap;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

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
  public EnqueueResult enqueue(byte [] queueName, QueueEntry entry,
                               long writeVersion) throws OperationException {
    return getQueue(queueName).enqueue(entry, writeVersion);
  }

  @Override
  public EnqueueResult enqueue(byte [] queueName, QueueEntry [] entries,
                               long writeVersion) throws OperationException {
    return getQueue(queueName).enqueue(entries, writeVersion);
  }

  @Override
  public void invalidate(byte [] queueName, QueueEntryPointer [] entryPointers,
      long writeVersion) throws OperationException {
    getQueue(queueName).invalidate(entryPointers, writeVersion);
  }

//  @Override
//  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer,
//      QueueConfig config, ReadPointer readPointer) throws OperationException {
//    return getQueue(queueName).dequeue(consumer, config, readPointer);
//  }

  @Override
  public DequeueResult dequeue(byte [] queueName, QueueConsumer consumer, ReadPointer readPointer)
                               throws OperationException {
    return getQueue(queueName).dequeue(consumer, readPointer);
  }

  @Override
  public void ack(byte[] queueName, QueueEntryPointer entryPointer, QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    getQueue(queueName).ack(entryPointer, consumer, readPointer);
  }

  @Override
  public void ack(byte[] queueName, QueueEntryPointer [] entryPointers, QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    getQueue(queueName).ack(entryPointers, consumer, readPointer);
  }

  @Override
  public void finalize(byte[] queueName, QueueEntryPointer [] entryPointers,
                       QueueConsumer consumer, int totalNumGroups, long writePoint) throws OperationException {
    getQueue(queueName).finalize(entryPointers, consumer, totalNumGroups, writePoint);
  }

  @Override
  public void unack(byte[] queueName, QueueEntryPointer [] entryPointers, QueueConsumer consumer, ReadPointer readPointer) throws OperationException {
    getQueue(queueName).unack(entryPointers, consumer, readPointer);
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
