package com.continuuity.data.operation.executor;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.ttqueue.*;

import java.util.List;
import java.util.Map;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

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
  public void execute(OperationContext context,
                      List<WriteOperation> writes) throws OperationException {
    // do nothing
  }

  @Override
  public DequeueResult execute(OperationContext context,
                               QueueDequeue dequeue) {
    // pretend the queue is empty
    return new DequeueResult(DequeueResult.DequeueStatus.EMPTY);
  }

  @Override
  public long execute(OperationContext context,
                      QueueAdmin.GetGroupID getGroupId) {
    return 0L;
  }

  @Override
  public OperationResult<QueueInfo>
  execute(OperationContext context, QueueAdmin.GetQueueInfo getQueueInfo) {
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
  public OperationResult<byte[]> execute(OperationContext context,
                                         ReadKey read)
      throws OperationException {
    // return empty result, key not found
    return new OperationResult<byte[]>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context, Read read) {
    // return empty result, key not found
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<List<byte[]>> execute(
      OperationContext context, ReadAllKeys readKeys) {
    return new OperationResult<List<byte[]>>(StatusCode.KEY_NOT_FOUND);

  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(OperationContext context, ReadColumnRange readColumnRange) {
    // pretend the key does not exists
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public void execute(OperationContext context, Write write)
      throws OperationException {
    // do nothing
  }

  @Override
  public void execute(OperationContext context,
                      Delete delete) throws OperationException {
    // do nothing
  }

  @Override
  public void execute(OperationContext context,
                      Increment inc) throws OperationException {
    // do nothing
  }

  @Override
  public void execute(OperationContext context,
                      CompareAndSwap cas) throws OperationException {
    // do nothing
  }

  @Override
  public void execute(OperationContext context,
                      QueueEnqueue enqueue) {
    // do nothing
  }

  @Override
  public void execute(OperationContext context,
                      QueueAck ack) {
    // do nothing
  }
}
