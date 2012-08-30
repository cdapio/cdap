package com.continuuity.data.operation.executor;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.ttqueue.*;

import java.util.List;
import java.util.Map;

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
  public void execute(List<WriteOperation> writes) throws OperationException {
    // do nothing
  }

  @Override
  public DequeueResult execute(QueueDequeue dequeue) {
    // pretend the queue is empty
    return new OperationResult<DequeueResult>(StatusCode.QUEUE_EMPTY);
  }

  @Override
  public OperationResult<Long> execute(QueueAdmin.GetGroupID getGroupId) {
    return new OperationResult<Long>(0);
  }

  @Override
  public OperationResult<QueueAdmin.QueueMeta>
  execute(QueueAdmin.GetQueueMeta getQueueMeta) {
    // pretend the queue does not exist
    return new OperationResult<QueueAdmin.QueueMeta>(
        StatusCode.QUEUE_NOT_FOUND);
  }

  @Override
  public void execute(ClearFabric clearFabric) {
    // do nothing
  }

  @Override
  public OperationResult<byte[]> execute(ReadKey read) {
    // return empty result, key not found
    return new OperationResult<byte[]>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> execute(Read read) {
    // return empty result, key not found
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public OperationResult<List<byte[]>> execute(ReadAllKeys readKeys) {
    return new OperationResult<List<byte[]>>(StatusCode.KEY_NOT_FOUND);

  }

  @Override
  public OperationResult<Map<byte[], byte[]>>
  execute(ReadColumnRange readColumnRange) {
    // pretend the key does not exists
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.KEY_NOT_FOUND);
  }

  @Override
  public void execute(Write write) {
    // do nothing
  }

  @Override
  public void execute(Delete delete) {
    // do nothing
  }

  @Override
  public void execute(Increment inc) {
    // do nothing
  }

  @Override
  public void execute(CompareAndSwap cas) throws OperationException {
    // do nothing
  }

  @Override
  public void execute(QueueEnqueue enqueue) {
    // do nothing
  }

  @Override
  public void execute(QueueAck ack) {
    // do nothing
  }
}
