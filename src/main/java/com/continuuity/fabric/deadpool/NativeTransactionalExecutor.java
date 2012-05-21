package com.continuuity.fabric.deadpool;

import java.util.Map;

import com.continuuity.data.operation.Modifier;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;

public interface NativeTransactionalExecutor extends NativeExecutor {

  // Reads

  public byte[] readRandom(byte[] key);

  public Map<byte[],byte[]> readOrdered(byte [] key);

  /**
   *
   * @param startKey inclusive
   * @param endKey exclusive
   * @return
   */
  public Map<byte[],byte[]> readOrdered(byte [] startKey, byte [] endKey);

  /**
   *
   * @param startKey inclusive
   * @param limit
   * @return
   */
  public Map<byte[],byte[]> readOrdered(byte [] startKey, int limit);

  public long readCounter(byte[] key);

  public long getCounter(byte [] key);
  
  // Writes

  public void writeRandom(byte [] key, byte [] value);

  public void writeOrdered(byte [] key, byte [] value);

  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue);

  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier);

  public long increment(byte [] key, long amount);

  public boolean queuePush(byte [] queueName, byte [] queueEntry);

  public boolean queueAck(byte [] queueName, QueueEntry queueEntry);

  public QueueEntry queuePop(byte [] queueName, QueueConsumer consumer,
      QueuePartitioner partitioner) throws InterruptedException;
}
