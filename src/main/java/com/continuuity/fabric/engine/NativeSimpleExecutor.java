package com.continuuity.fabric.engine;

import java.util.Map;

import com.continuuity.fabric.operations.impl.Modifier;
import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public interface NativeSimpleExecutor extends NativeExecutor {

  public byte[] readRandom(byte[] key);

  public void writeRandom(byte [] key, byte [] value);

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

  public void writeOrdered(byte [] key, byte [] value);

  public long readCounter(byte[] key);

  public boolean compareAndSwap(byte [] key,
      byte [] expectedValue, byte [] newValue);

  public void readModifyWrite(byte [] key, Modifier<byte[]> modifier);

  public long increment(byte [] key, long amount);

  public long getCounter(byte [] key);

  public boolean queuePush(byte [] queueName, byte [] queueEntry);

  public boolean queueAck(byte [] queueName, QueueEntry queueEntry);

  public QueueEntry queuePop(byte [] queueName, QueueConsumer consumer,
      QueuePartitioner partitioner) throws InterruptedException;
}
