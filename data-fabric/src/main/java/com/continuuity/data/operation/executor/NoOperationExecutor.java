package com.continuuity.data.operation.executor;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.TruncateTable;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups;
import com.continuuity.data.operation.ttqueue.admin.QueueDropInflight;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.data.table.Scanner;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is an implementation of OperationExecutor that does nothing but
 * return empty results. It is useful for testing and performance benchmarks.
 */
public class NoOperationExecutor implements OperationExecutor {

  @Override
  public String getName() {
    return "noop";
  }

  @Override
  public void commit(OperationContext context, List<WriteOperation> writes)
    throws OperationException {
    // do nothing
  }

  @Override
  public Transaction startTransaction(OperationContext context, boolean trackChanges)
    throws OperationException {
    return null;
  }

  @Override
  public Transaction execute(OperationContext context,
                             Transaction transaction,
                             List<WriteOperation> writes)
    throws OperationException {
    commit(context, writes);
    return null;
  }

  @Override
  public void commit(OperationContext context,
                     Transaction transaction)
    throws OperationException {
    // do nothing
  }

  @Override
  public void commit(OperationContext context,
                     Transaction transaction,
                     List<WriteOperation> writes)
    throws OperationException {
    commit(context, writes);
  }

  @Override
  public void abort(OperationContext context,
                    Transaction transaction)
    throws OperationException {
    // do nothing
  }

  @Override
  public Map<byte[], Long> increment(OperationContext context, Increment increment)
    throws OperationException {
    // do nothing and return nothing
    return Collections.emptyMap();
  }

  @Override
  public Map<byte[], Long> increment(OperationContext context,
                                     Transaction transaction,
                                     Increment increment)
    throws OperationException {
    // do nothing and return nothing
    return Collections.emptyMap();
  }

  @Override
  public DequeueResult execute(OperationContext context,
                               QueueDequeue dequeue) {
    // pretend the queue is empty
    return new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
  }

  @Override
  public long execute(OperationContext context,
                      GetGroupID getGroupId) {
    return 0L;
  }

  @Override
  public OperationResult<QueueInfo> execute(OperationContext context,
                                            GetQueueInfo getQueueInfo) {
    // pretend the queue does not exist
    return new OperationResult<QueueInfo>(StatusCode.QUEUE_NOT_FOUND);
  }

  @Override
  public void execute(OperationContext context,
                      ClearFabric clearFabric) {
    // do nothing
  }

  @Override
  public void execute(OperationContext context,
                      OpenTable openTable) throws OperationException {
    // do nothing
  }

  @Override
  public void execute(OperationContext context, TruncateTable truncateTable) throws OperationException {
    // do nothing
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context, Read read)
    throws OperationException {
    // return empty result, key not found
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      Read read)
    throws OperationException {
    return execute(context, read);
  }

  @Override
  public OperationResult<List<byte[]>> execute(
    OperationContext context, ReadAllKeys readKeys) {
    return new OperationResult<List<byte[]>>(StatusCode.KEY_NOT_FOUND);

  }

  @Override
  public OperationResult<List<byte[]>> execute(OperationContext context,
                                               Transaction transaction,
                                               ReadAllKeys readKeys)
    throws OperationException {
    return execute(context, readKeys);
  }

  @Override
  public OperationResult<List<KeyRange>> execute(OperationContext context,
                                                 GetSplits getSplits)
    throws OperationException {
      return new OperationResult<List<KeyRange>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<List<KeyRange>> execute(OperationContext context,
                                                 Transaction transaction,
                                                 GetSplits getSplits)
    throws OperationException {
    return execute(context, getSplits);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context, ReadColumnRange readColumnRange) {
    // pretend the key does not exists
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(OperationContext context,
                                                      Transaction transaction,
                                                      ReadColumnRange readColumnRange)
    throws OperationException {
    return execute(context, readColumnRange);
  }

  @Override
  public void execute(OperationContext context, QueueConfigure configure)
    throws OperationException {
    // Nothing to do
  }

  @Override
  public void execute(OperationContext context, QueueConfigureGroups configure) throws OperationException {
    // Nothing to do
  }

  @Override
  public void execute(OperationContext context, QueueDropInflight op) throws OperationException {
    // Noting to do
  }

  @Override
  public Scanner scan(OperationContext context, @Nullable Transaction transaction, Scan scan)
    throws OperationException {
    return new Scanner() {
      @Override
      public ImmutablePair<byte[], Map<byte[], byte[]>> next() {
        return null;
      }
      @Override
      public void close() {
        // nothing to do
      }
    };
  }

  // Dataset and queue logic relies on tx id to grow monotonically even after restart. Hence we need to start with
  // value that is for sure bigger than the last one used before restart.
  // NOTE: with code below we assume we don't do more than 100K tx/sec
  private AtomicLong tx = new AtomicLong(System.currentTimeMillis() * 100);

  @Override
  public com.continuuity.data2.transaction.Transaction start() throws OperationException {
    long wp = tx.incrementAndGet();
    // NOTE: -1 here is because we have logic that uses (readpointer + 1) as a "exclusive stop key" in some datasets
    return new com.continuuity.data2.transaction.Transaction(Long.MAX_VALUE - 1, wp, new long[0], new long[0]);
  }

  @Override
  public com.continuuity.data2.transaction.Transaction start(Integer timeout) throws OperationException {
    long wp = tx.incrementAndGet();
    return new com.continuuity.data2.transaction.Transaction(Long.MAX_VALUE - 1, wp, new long[0], new long[0]);
  }

  @Override
  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException {
    return true;
  }

  @Override
  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    return true;
  }

  @Override
  public boolean abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    return true;
  }

  @Override
  public void commit(OperationContext context, WriteOperation write)
    throws OperationException {
    // do nothing
  }

}
