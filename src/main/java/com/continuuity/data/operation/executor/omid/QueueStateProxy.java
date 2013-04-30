package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.util.RowLockTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;

/**
 * This proxy runs all queue operations while managing queue state for each consumer.
 * It caches the queue state internally, and makes it available for any queue operation run using this executor.
 * It also makes sure to run the queue operations for a single consumer in a serial manner.
 */
public class QueueStateProxy {
  private final RowLockTable locks = new RowLockTable();
  private final ConcurrentMap<RowLockTable.Row, StatefulQueueConsumer> statePool = Maps.newConcurrentMap();

  /**
   * Used to run a queue operation that returns a value
   * @param queueName Queue name on which to run the operation
   * @param queueConsumer Queue consumer for queueName (without state)
   * @param op Queue operation that needs to be run
   * @param <T> Type of return value
   * @return The value returned by operation op
   * @throws OperationException
   */
  public <T> T call(byte[] queueName, QueueConsumer queueConsumer, QueueCallable<T> op)
    throws OperationException {
    Preconditions.checkArgument(op != null && queueConsumer != null);

    ConsumerHolder consumerHolder = null;
    try {
      consumerHolder = checkout(queueName, queueConsumer);
      //noinspection ConstantConditions
      return op.call(consumerHolder.statefulQueueConsumer);
    } finally {
      if(consumerHolder != null) {
        //noinspection ConstantConditions
        release(consumerHolder, op.statefulQueueConsumer);
      }
    }
  }

  /**
   * Used to run a queue operation that does not return anything
   * @param queueName Queue name on which to run the operation
   * @param queueConsumer Queue consumer for queueName (without state)
   * @param op Queue operation that needs to be run
   * @throws OperationException
   */
  public void run(byte[] queueName, QueueConsumer queueConsumer, QueueRunnable op)
    throws OperationException {
    Preconditions.checkArgument(op != null && queueConsumer != null);

    ConsumerHolder consumerHolder = null;
    try {
      consumerHolder = checkout(queueName, queueConsumer);
      //noinspection ConstantConditions
      op.run(consumerHolder.statefulQueueConsumer);
    } finally {
      if(consumerHolder != null) {
        //noinspection ConstantConditions
        release(consumerHolder, op.statefulQueueConsumer);
      }
    }
  }

  /**
   * A QueueCallable is used to define a queue operation that returns a value.
   * QueueCallable will be run by the QueueStateProxy.
   * @param <T> Type of the return value
   */
  public static abstract class QueueCallable<T> {
    private volatile StatefulQueueConsumer statefulQueueConsumer = null;

    /**
     * The call method will be called when the QueueCallable is executed.
     * @param statefulQueueConsumer QueueConsumer with state that needs to be passed to all queue methods.
     * @return Return value of the queue operation
     * @throws OperationException
     */
    public abstract T call(StatefulQueueConsumer statefulQueueConsumer) throws OperationException;

    /**
     * If the statefulQueueConsumer changes outside of the JVM during the queue operation then
     * this method is used let QueueStateProxy know about the change.
     * @param statefulQueueConsumer Updated statefulQueueConsumer
     */
    @SuppressWarnings("UnusedDeclaration")
    public void updateStatefulConsumer(StatefulQueueConsumer statefulQueueConsumer) {
      this.statefulQueueConsumer = statefulQueueConsumer;
    }
  }

  /**
   * A QueueRunnable is used to define a queue operation that does not return a value.
   * QueueRunnable will be run by the QueueStateProxy.
   */
  public static abstract class QueueRunnable {
    private volatile StatefulQueueConsumer statefulQueueConsumer = null;

    /**
     * The run method will be called when the QueueRunnable is executed.
     * @param statefulQueueConsumer QueueConsumer with state that needs to be passed to all queue methods.
     * @throws OperationException
     */
    public abstract void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException;

    /**
     * If the statefulQueueConsumer changes outside of the JVM during the queue operation then
     * this method is used let QueueStateProxy know about the change.
     * @param statefulQueueConsumer Updated statefulQueueConsumer
     */
    @SuppressWarnings("UnusedDeclaration")
    public void updateStatefulConsumer(StatefulQueueConsumer statefulQueueConsumer) {
      this.statefulQueueConsumer = statefulQueueConsumer;
    }
  }

  private ConsumerHolder checkout(byte[] queueName, QueueConsumer queueConsumer) {
    RowLockTable.Row row = new RowLockTable.Row(getKey(queueName, queueConsumer));
    StatefulQueueConsumer statefulQueueConsumer = null;

    // first obtain the lock
    RowLockTable.RowLock lock;
    do {
      lock = this.locks.lock(row);
      // obtained a lock, but it may be invalid, loop until valid
    } while (!lock.isValid());

    // We now have the lock, get the state
    if(queueConsumer.isStateInitialized()) {
      // This consumer has state
      // We remove the consumer from pool rather than get, this will reduce state corruption due to exceptions, etc.
      // It is safer to return an empty state rather than a stale one.
      statefulQueueConsumer = statePool.remove(row);
      // Note: Even though the consumer says its state is initialized, we may still not be able to find the state
      // in the pool. This could happen if the consumer's previous operation threw an exception and the state was
      // not returned properly.
      // Create a fresh state object and return it in such cases too.
    }
    if(statefulQueueConsumer == null) {
      // New consumer, or consumer crashed
      // Create a new stateful consumer
      statefulQueueConsumer = new StatefulQueueConsumer(queueConsumer.getInstanceId(),
                                                        queueConsumer.getGroupId(),
                                                        queueConsumer.getGroupSize(),
                                                        queueConsumer.getGroupName(),
                                                        queueConsumer.getPartitioningKey(),
                                                        queueConsumer.getQueueConfig());
    }
    queueConsumer.setStateInitialized(true);
    // Note: we still have the lock
    return new ConsumerHolder(row, lock, statefulQueueConsumer);
  }

  private void release(ConsumerHolder consumerHolder, StatefulQueueConsumer statefulQueueConsumer) {
    if(consumerHolder == null) {
      return;
    }

    StatefulQueueConsumer toReturnConsumer;
    /**
     * If the operation sends an update to statefulQueueConsumer,  use it in preference to the one we have
     */
    if(statefulQueueConsumer != null) {
      toReturnConsumer = statefulQueueConsumer;
    } else {
      toReturnConsumer = consumerHolder.statefulQueueConsumer;
    }

    // Store the consumer state
    statePool.put(consumerHolder.row, toReturnConsumer);

    // If lock and row are valid, unlock
    if(consumerHolder.lock != null && consumerHolder.lock.isValid()) {
      locks.unlock(consumerHolder.row);
    }
  }

  private byte[] getKey(byte[] queueName, QueueConsumer queueConsumer) {
    return Bytes.add(
      Bytes.toBytes(queueConsumer.getGroupId()), Bytes.toBytes(queueConsumer.getInstanceId()), queueName
    );
  }

  private static class ConsumerHolder {
    private final RowLockTable.Row row;
    private final RowLockTable.RowLock lock;
    private StatefulQueueConsumer statefulQueueConsumer;

    private ConsumerHolder(RowLockTable.Row row, RowLockTable.RowLock lock,
                           StatefulQueueConsumer statefulQueueConsumer) {
      this.row = row;
      this.lock = lock;
      this.statefulQueueConsumer = statefulQueueConsumer;
    }
  }
}
