package com.continuuity.data.operation.executor;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.ttqueue.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

public class NoOperationExecutor implements OperationExecutor {

  private static final byte[] keyA = { 'a' };
  private static final byte[] valueOne = { 1 };

  @Override
  public String getName() {
    return "noop";
  }

  @Override
  public BatchOperationResult execute(List<WriteOperation> writes) throws BatchOperationException {
    return new BatchOperationResult(true);
  }

  @Override
  public DequeueResult execute(QueueDequeue dequeue) {
    return new DequeueResult(
        DequeueResult.DequeueStatus.SUCCESS,
        new QueueEntryPointer(dequeue.getKey(), 1, 1),
        valueOne);
  }

  @Override
  public long execute(QueueAdmin.GetGroupID getGroupId) {
    return 0;
  }

  @Override
  public QueueAdmin.QueueMeta execute(QueueAdmin.GetQueueMeta getQueueMeta) {
    return new QueueAdmin.QueueMeta();
  }

  @Override
  public void execute(ClearFabric clearFabric) {
  }

  @Override
  public byte[] execute(ReadKey read) {
    return valueOne;
  }

  @Override
  public Map<byte[], byte[]> execute(Read read) {
    Map<byte[], byte[]> map = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    map.put(keyA, valueOne);
    return map;
  }

  @Override
  public List<byte[]> execute(ReadAllKeys readKeys) {
    List<byte[]> list = Lists.newLinkedList();
    list.add(keyA);
    return list;
  }

  @Override
  public Map<byte[], byte[]> execute(ReadColumnRange readColumnRange) {
    Map<byte[], byte[]> map = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    map.put(keyA, valueOne);
    return map;
  }

  @Override
  public boolean execute(Write write) {
    return true;
  }

  @Override
  public boolean execute(Delete delete) {
    return true;
  }

  @Override
  public boolean execute(Increment inc) {
    return true;
  }

  @Override
  public boolean execute(CompareAndSwap cas) {
    return true;
  }

  @Override
  public boolean execute(QueueEnqueue enqueue) {
    return true;
  }

  @Override
  public boolean execute(QueueAck ack) {
    return true;
  }
}
