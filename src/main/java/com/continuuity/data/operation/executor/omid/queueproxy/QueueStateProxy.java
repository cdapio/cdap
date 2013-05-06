package com.continuuity.data.operation.executor.omid.queueproxy;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.util.RowLockTable;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.util.concurrent.TimeUnit;

/**
 * This proxy runs all queue operations while managing queue state for each consumer.
 * It caches the queue state internally, and makes it available for any queue operation run using this executor.
 * It also makes sure to run the queue operations for a single consumer in a serial manner.
 */
public class QueueStateProxy {
  private final RowLockTable locks = new RowLockTable();
  private final Cache<RowLockTable.Row, StatefulQueueConsumer> stateCache;

  public QueueStateProxy(final long maxSizeBytes) {
    this.stateCache =
    CacheBuilder
      .newBuilder()
      .initialCapacity(1000)
      .expireAfterAccess(1, TimeUnit.HOURS)
      .maximumWeight(maxSizeBytes)
      .weigher(new Weigher<RowLockTable.Row, StatefulQueueConsumer>() {
        @Override
        public int weigh(RowLockTable.Row key, StatefulQueueConsumer value) {
          int weight = 32; // small constant representing key
          if (value.getQueueState() != null) {
            weight += value.getQueueState().weight();
          }
          return weight;
        }
      })
      .build();
  }

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
    return runOperation(queueName, queueConsumer, op);
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
    runOperation(queueName, queueConsumer, op);
  }

  private <T> T runOperation(byte[] queueName, QueueConsumer queueConsumer, QueueOperation<T> op)
    throws OperationException {
    Preconditions.checkArgument(op != null && queueConsumer != null);

    ConsumerHolder consumerHolder = null;
    try {
      consumerHolder = checkout(queueName, queueConsumer);
      // op is already asserted to be not null!
      //noinspection ConstantConditions
      return op.execute(consumerHolder.statefulQueueConsumer);
    } finally {
      if(consumerHolder != null) {
        // If the operation sends an update to statefulQueueConsumer,  use it in preference to the one we have
        // op is already asserted to be not null!
        //noinspection ConstantConditions
        if(op.statefulQueueConsumer != null) {
          consumerHolder.statefulQueueConsumer = op.statefulQueueConsumer;
        }
        release(consumerHolder);
      }
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
    if(queueConsumer.getStateType() == QueueConsumer.StateType.INITIALIZED) {
      // This consumer has state
      // We remove the consumer from pool rather than get, this will reduce state corruption due to exceptions, etc.
      // It is safer to return an empty state rather than a stale one.
      statefulQueueConsumer = stateCache.getIfPresent(row);
      stateCache.invalidate(row);
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
      // If consumer says state was initialized, then it might have been evicted from cache
      if(queueConsumer.getStateType() == QueueConsumer.StateType.INITIALIZED) {
        statefulQueueConsumer.setStateType(QueueConsumer.StateType.NOT_FOUND);
      }
    }
    queueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
    // Note: we still have the lock
    return new ConsumerHolder(row, lock, statefulQueueConsumer);
  }

  private void release(ConsumerHolder consumerHolder) {
    if(consumerHolder == null) {
      return;
    }

    // Store the consumer state
    stateCache.put(consumerHolder.row, consumerHolder.statefulQueueConsumer);

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
    private volatile StatefulQueueConsumer statefulQueueConsumer;

    private ConsumerHolder(RowLockTable.Row row, RowLockTable.RowLock lock,
                           StatefulQueueConsumer statefulQueueConsumer) {
      this.row = row;
      this.lock = lock;
      this.statefulQueueConsumer = statefulQueueConsumer;
    }
  }
}
