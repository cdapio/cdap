package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.remote.stubs.*;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.operation.ttqueue.internal.ExecutionMode;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TBaseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConverterUtils {

  private static final Logger Log =
      LoggerFactory.getLogger(ConverterUtils.class);

  /** wrap an array of longs into a list of Long objects */
  List<Long> wrap(long[] array) {
    List<Long> list = new ArrayList<Long>(array.length);
    for (long num : array) list.add(num);
    return list;
  }
  /** unwrap an array of longs from a list of Long objects */
  long[] unwrap(List<Long> list) {
    long[] longs = new long[list.size()];
    int i = 0;
    for (Long value : list)
      longs[i++] = value;
    return longs;
  }

  /** wrap a byte array into a byte buffer */
  ByteBuffer wrap(byte[] bytes) {
    if (bytes == null)
      return null;
    else
      return ByteBuffer.wrap(bytes);
  }
  /** unwrap a byte array from a byte buffer */
  byte[] unwrap(ByteBuffer buf) {
    if (buf == null)
      return null;
    else
      return TBaseHelper.byteBufferToByteArray(buf);
  }

  /** wrap a byte array into an optional binary */
  TOptionalBinary wrapBinary(byte[] value) {
    TOptionalBinary binary = new TOptionalBinary();
    if (value != null)
      binary.setValue(value);
    return binary;
  }
  /** unwrap a byte array from an optional binary */
  byte[] unwrap(TOptionalBinary binary) {
    return binary.getValue();
  }

  /** wrap an array of byte arrays into a list of byte buffers */
  List<ByteBuffer> wrap(byte[][] arrays) {
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(arrays.length);
    for (byte[] array : arrays)
      buffers.add(wrap(array));
    return buffers;
  }
  /** wrap an list of byte arrays into a list of byte buffers */
  List<ByteBuffer> wrap(List<byte[]> arrays) {
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(arrays.size());
    for (byte[] array : arrays)
      buffers.add(wrap(array));
    return buffers;
  }
  /** unwrap an array of byte arrays from a list of byte buffers */
  byte[][] unwrap(List<ByteBuffer> buffers) {
    byte[][] arrays = new byte[buffers.size()][];
    int i = 0;
    for (ByteBuffer buffer : buffers)
      arrays[i++] = unwrap(buffer);
    return arrays;
  }
  /** unwrap an array of byte arrays from a list of byte buffers */
  List<byte[]> unwrapList(List<ByteBuffer> buffers) {
    List<byte[]> arrays = new ArrayList<byte[]>(buffers.size());
    for (ByteBuffer buffer : buffers)
      arrays.add(unwrap(buffer));
    return arrays;
  }

  /** wrap a map of byte arrays into an optional map of byte buffers */
  TOptionalBinaryList wrapList(List<byte[]> list) {
    TOptionalBinaryList opt = new TOptionalBinaryList();
    if (list != null)
      opt.setTheList(wrap(list));
    return opt;
  }
  /** unwrap an optional map of byte buffers */
  List<byte[]> unwrap(TOptionalBinaryList opt) {
    return unwrapList(opt.getTheList());
  }

  /** wrap a map of byte arrays into a map of byte buffers */
  Map<ByteBuffer, TOptionalBinary> wrap(Map<byte[], byte[]> map) {
    if (map == null)
      return null;
    Map<ByteBuffer, TOptionalBinary> result = Maps.newHashMap();
    for(Map.Entry<byte[], byte[]> entry : map.entrySet())
      result.put(wrap(entry.getKey()), wrapBinary(entry.getValue()));
    return result;
  }
  /** unwrap a map of byte arrays from a map of byte buffers */
  Map<byte[], byte[]> unwrap(Map<ByteBuffer, TOptionalBinary> map) {
    if (map == null)
      return null;
    Map<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<ByteBuffer, TOptionalBinary> entry : map.entrySet())
      result.put(unwrap(entry.getKey()), unwrap(entry.getValue()));
    return result;
  }

  /** wrap a map of byte arrays into an optional map of byte buffers */
  TOptionalBinaryMap wrapMap(Map<byte[], byte[]> map) {
    TOptionalBinaryMap opt = new TOptionalBinaryMap();
    if (map != null)
      opt.setTheMap(wrap(map));
    return opt;
  }
  /** unwrap an optional map of byte buffers */
  Map<byte[], byte[]> unwrap(TOptionalBinaryMap opt) {
    return unwrap(opt.getTheMap());
  }

  /** wrap a ClearFabric operation */
  TClearFabric wrap(ClearFabric clearFabric) {
    return new TClearFabric(
        clearFabric.shouldClearData(),
        clearFabric.shouldClearQueues(),
        clearFabric.shouldClearStreams());
  }
  /** unwrap a ClearFabric operation */
  ClearFabric unwrap(TClearFabric clearFabric) {
    return new ClearFabric(
        clearFabric.isClearData(),
        clearFabric.isClearQueues(),
        clearFabric.isClearStreams());
  }

  /** wrap a Write operation */
  TWrite wrap(Write write) {
    return new TWrite(
        wrap(write.getKey()),
        wrap(write.getColumns()),
        wrap(write.getValues()));
  }
  /** unwrap a Write operation */
  Write unwrap(TWrite tWrite) {
    return new Write(
        tWrite.getKey(),
        unwrap(tWrite.getColumns()),
        unwrap(tWrite.getValues()));
  }

  /** wrap a Delete operation */
  TDelete wrap(Delete delete) {
    return new TDelete(
        wrap(delete.getKey()),
        wrap(delete.getColumns()));
  }
  /** unwrap a Delete operation */
  Delete unwrap(TDelete tDelete) {
    return new Delete(
        tDelete.getKey(),
        unwrap(tDelete.getColumns()));
  }

  /** wrap an Increment operation */
  TIncrement wrap(Increment increment) {
    return new TIncrement(
        wrap(increment.getKey()),
        wrap(increment.getColumns()),
        wrap(increment.getAmounts()));
  }
  /** unwrap an Increment operation */
  Increment unwrap(TIncrement tIncrement) {
    return new Increment(
        tIncrement.getKey(),
        unwrap(tIncrement.getColumns()),
        unwrap(tIncrement.getAmounts()));
  }

  /** wrap a CompareAndSwap operation */
  TCompareAndSwap wrap(CompareAndSwap compareAndSwap) {
    return new TCompareAndSwap(
        wrap(compareAndSwap.getKey()),
        wrap(compareAndSwap.getColumn()),
        wrap(compareAndSwap.getExpectedValue()),
        wrap(compareAndSwap.getNewValue()));
  }
  /** unwrap a CompareAndSwap operation */
  CompareAndSwap unwrap(TCompareAndSwap tCompareAndSwap) {
    return new CompareAndSwap(
        tCompareAndSwap.getKey(),
        tCompareAndSwap.getColumn(),
        tCompareAndSwap.getExpectedValue(),
        tCompareAndSwap.getNewValue());
  }

  /** wrap a Read operation */
  TRead wrap(Read read) {
    return new TRead(
        wrap(read.getKey()),
        wrap(read.getColumns()));
  }
  /** unwrap a Read operation */
  Read unwrap(TRead tRead) {
    return new Read(
        tRead.getKey(),
        unwrap(tRead.getColumns()));
  }

  /** wrap a ReadKey operation */
  TReadKey wrap(ReadKey readKey) {
    return new TReadKey(
        wrap(readKey.getKey()));
  }
  /** unwrap a ReadKey operation */
  ReadKey unwrap(TReadKey tReadKey) {
    return new ReadKey(
        tReadKey.getKey());
  }

  /** wrap a ReadAllKeys operation */
  TReadAllKeys wrap(ReadAllKeys readAllKeys) {
    return new TReadAllKeys(
        readAllKeys.getOffset(),
        readAllKeys.getLimit());
  }
  /** unwrap a ReadAllKeys operation */
  ReadAllKeys unwrap(TReadAllKeys tReadAllKeys) {
    return new ReadAllKeys(
        tReadAllKeys.getOffset(),
        tReadAllKeys.getLimit());
  }

  /** wrap a ReadColumnRange operation */
  TReadColumnRange wrap(ReadColumnRange readColumnRange) {
    return new TReadColumnRange(
        wrap(readColumnRange.getKey()),
        wrap(readColumnRange.getStartColumn()),
        wrap(readColumnRange.getStopColumn()),
        readColumnRange.getLimit());
  }
  /** unwrap a ReadColumnRange operation */
  ReadColumnRange unwrap(TReadColumnRange tReadColumnRange) {
    return new ReadColumnRange(
        tReadColumnRange.getKey(),
        tReadColumnRange.getStartColumn(),
        tReadColumnRange.getStopColumn());
  }

  /** wrap an Enqueue operation */
  TQueueEnqueue wrap(QueueEnqueue enqueue) {
    return new TQueueEnqueue(
        wrap(enqueue.getKey()),
        wrap(enqueue.getData()));
  }
  /** unwrap an Enqueue operation */
  QueueEnqueue unwrap(TQueueEnqueue tEnqueue) {
    return new QueueEnqueue(
        tEnqueue.getQueueName(),
        tEnqueue.getValue());
  }

  /** wrap a Dequeue operation */
  TQueueDequeue wrap(QueueDequeue dequeue) {
    return new TQueueDequeue(
        wrap(dequeue.getKey()),
        wrap(dequeue.getConsumer()),
        wrap(dequeue.getConfig()));
  }
  /** unwrap a Dequeue operation */
  QueueDequeue unwrap(TQueueDequeue dequeue) {
    return new QueueDequeue(dequeue.getQueueName(),
        unwrap(dequeue.getConsumer()),
        unwrap(dequeue.getConfig()));
  }

  /** wrap a QueueAck operation */
  TQueueAck wrap(QueueAck ack) {
    return new TQueueAck(
        wrap(ack.getKey()),
        wrap(ack.getEntryPointer()),
        wrap(ack.getConsumer()),
        ack.getNumGroups());
  }
  /** unwrap a QueueAck operation */
  QueueAck unwrap(TQueueAck tQueueAck) {
    return new QueueAck(
        tQueueAck.getQueueName(),
        unwrap(tQueueAck.getEntryPointer()),
        unwrap(tQueueAck.getConsumer()),
        tQueueAck.getNumGroups());
  }

  /** wrap a GetQueueMeta operation */
  TGetQueueMeta wrap(QueueAdmin.GetQueueMeta getQueueMeta) {
    return new TGetQueueMeta(wrap(getQueueMeta.getQueueName()));
  }
  /** unwrap a GetQueueMeta operation */
  QueueAdmin.GetQueueMeta unwrap(TGetQueueMeta tGetQueueMeta) {
    return new QueueAdmin.GetQueueMeta(tGetQueueMeta.getQueueName());
  }

  /** wrap a GetGroupId operation */
  TGetGroupId wrap(QueueAdmin.GetGroupID getGroupId) {
    return new TGetGroupId(wrap(getGroupId.getQueueName()));
  }
  /** unwrap a GetGroupId operation */
  QueueAdmin.GetGroupID unwrap(TGetGroupId tGetGroupId) {
    return new QueueAdmin.GetGroupID(tGetGroupId.getQueueName());
  }

  /** wrap a queue entry pointer */
  TQueueEntryPointer wrap(QueueEntryPointer entryPointer) {
    return new TQueueEntryPointer(
        wrap(entryPointer.getQueueName()),
        entryPointer.getEntryId(),
        entryPointer.getShardId());
  }
  /** unwrap a queue entry pointer */
  QueueEntryPointer unwrap(TQueueEntryPointer tPointer) {
    return new QueueEntryPointer(
        tPointer.getQueueName(),
        tPointer.getEntryId(),
        tPointer.getShardId());
  }
  /** wrap a queue entry pointer (to an entry pointer) */
  EntryPointer unwrapEntryPointer(TQueueEntryPointer tPointer) {
    return new EntryPointer(
        tPointer.getEntryId(),
        tPointer.getShardId());
  }

  /** wrap a queue consumer */
  TQueueConsumer wrap(QueueConsumer consumer) {
    return new TQueueConsumer(
        consumer.getInstanceId(),
        consumer.getGroupId(),
        consumer.getGroupSize());
  }
  /** unwrap a queue consumer */
  QueueConsumer unwrap(TQueueConsumer tQueueConsumer) {
    return new QueueConsumer(
        tQueueConsumer.getInstanceId(),
        tQueueConsumer.getGroupId(),
        tQueueConsumer.getGroupSize());
  }

  /**
   * wrap a queue partitioner. This can be a little tricky: in
   * Thrift, this is an enum, whereas in data fabric, this is
   * an actual class (it could be custom implementation by the
   * caller). If we find a partitioner that is not predefined
   * by data fabric, we log an error and default to random.
   */
  TQueuePartitioner wrap(QueuePartitioner partitioner) {
    if (partitioner instanceof QueuePartitioner.HashPartitioner)
      return TQueuePartitioner.HASH;
    if (partitioner instanceof QueuePartitioner.RandomPartitioner)
      return TQueuePartitioner.RANDOM;
    Log.error("Internal Error: Received an unknown QueuePartitioner with " +
        "class " + partitioner.getClass().getName() + ". Defaulting to " +
        QueuePartitioner.RandomPartitioner.class.getName());
    return TQueuePartitioner.RANDOM;
  }
  /**
   * unwrap a queue partitioner. We can only do this for the known
   * instances of the Thrift enum. If we encounter something else,
   * we log an error and fall back to random.
   */
  QueuePartitioner unwrap(TQueuePartitioner tPartitioner) {
    if (TQueuePartitioner.HASH.equals(tPartitioner))
      return new QueuePartitioner.HashPartitioner();
    if (TQueuePartitioner.RANDOM.equals(tPartitioner))
      return new QueuePartitioner.RandomPartitioner();
    Log.error("Internal Error: Received unknown QueuePartitioner " +
        tPartitioner + ". Defaulting to " + TQueuePartitioner.RANDOM + ".");
    return new QueuePartitioner.RandomPartitioner();
  }

  /** wrap a queue config */
  TQueueConfig wrap(QueueConfig config) {
    return new TQueueConfig(
        wrap(config.getPartitioner()),
        config.isSingleEntry());
  }
  /** unwrap a queue config */
  QueueConfig unwrap(TQueueConfig config) {
    return new QueueConfig(
        unwrap(config.getPartitioner()),
        config.isSingleEntry());
  }

  /** wrap a (queue) execution mode. If the mode is unknown, return null. */
  TExecutionMode wrap(ExecutionMode mode) {
    if (ExecutionMode.SINGLE_ENTRY.equals(mode))
      return TExecutionMode.SINGLE_ENTRY;
    if (ExecutionMode.MULTI_ENTRY.equals(mode))
      return TExecutionMode.MULTI_ENTRY;
    Log.error("Internal Error: Received unknown ExecutionMode " + mode);
    return null;
  }
  /** unwrap a (queue) execution mode. If the mode is unknown, return null. */
  ExecutionMode unwrap(TExecutionMode mode) {
    if (TExecutionMode.SINGLE_ENTRY.equals(mode))
      return ExecutionMode.SINGLE_ENTRY;
    if (TExecutionMode.MULTI_ENTRY.equals(mode))
      return ExecutionMode.MULTI_ENTRY;
    Log.error("Internal Error: Received unknown TExecutionMode " + mode);
    return null;
  }

  /** wrap the group states of a queue meta */
  List<TGroupState> wrap(GroupState[] groupStates) {
    List<TGroupState> list = new ArrayList<TGroupState>(groupStates.length);
    for (GroupState groupState : groupStates) {
      TExecutionMode tMode = wrap(groupState.getMode());
      if (tMode == null)
        continue;
      list.add(new TGroupState(
          groupState.getGroupSize(),
          wrap(groupState.getHead()),
          tMode));
    }
    return list;
  }
  /** wrap the group states of a queue meta */
  GroupState[] unwrap(List<TGroupState> tGroupStates) {
    ArrayList<GroupState> groups = Lists.newArrayList();
    for (TGroupState tGroupState : tGroupStates) {
      ExecutionMode mode = unwrap(tGroupState.getMode());
      if (mode == null)
        continue;
      groups.add(new GroupState(
          tGroupState.getGroupSize(),
          unwrapEntryPointer(tGroupState.getHead()),
          mode));
    }
    return groups.toArray(new GroupState[groups.size()]);
  }

  /**
   * wrap a queue meta.
   * The first field of TQueueMeta indicates whether this represents null
   */
  TQueueMeta wrap(QueueAdmin.QueueMeta meta) {
    if (meta == null)
      return new TQueueMeta(true, 0L, 0L, null);
    return new TQueueMeta(
        false,
        meta.getGlobalHeadPointer(),
        meta.getCurrentWritePointer(),
        wrap(meta.getGroups()));
  }
  /** wrap a queue meta */
  QueueAdmin.QueueMeta unwrap(TQueueMeta tQueueMeta) {
    if (tQueueMeta.isNulled())
      return null;
    return new QueueAdmin.QueueMeta(
        tQueueMeta.getGlobalHeadPointer(),
        tQueueMeta.getCurrentWritePointer(),
        unwrap(tQueueMeta.getGroups()));
  }

  /**
   * wrap a dequeue result.
   * If the status is unknown, return failure status and appropriate message.
   */
  TDequeueResult wrap(DequeueResult result) {
    TDequeueStatus status;
    if (DequeueResult.DequeueStatus.EMPTY.equals(result.getStatus()))
      status = TDequeueStatus.EMPTY;
    else if (DequeueResult.DequeueStatus.FAILURE.equals(result.getStatus()))
      status = TDequeueStatus.FAILURE;
    else if (DequeueResult.DequeueStatus.RETRY.equals(result.getStatus()))
      status = TDequeueStatus.RETRY;
    else if (DequeueResult.DequeueStatus.SUCCESS.equals(result.getStatus()))
      status = TDequeueStatus.SUCCESS;
    else {
      String message = "Internal Error: Received an unknown dequeue status of "
          + result.getStatus() + ".";
      Log.error(message);
      return new TDequeueResult(TDequeueStatus.FAILURE, null, null, message);
    }
    return new TDequeueResult(status,
        wrap(result.getEntryPointer()),
        wrap(result.getValue()),
        result.getMsg());
  }
  /**
   * unwrap a dequeue result
   * If the status is unknown, return failure status and appropriate message.
   */
  DequeueResult unwrap(TDequeueResult tDequeueResult) {
    if (tDequeueResult.getStatus().equals(TDequeueStatus.SUCCESS)) {
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
          unwrap(tDequeueResult.getPointer()),
          tDequeueResult.getValue());
    } else {
      DequeueResult.DequeueStatus status;
      TDequeueStatus tStatus = tDequeueResult.getStatus();
      if (TDequeueStatus.EMPTY.equals(tStatus))
        status = DequeueResult.DequeueStatus.EMPTY;
      else if (TDequeueStatus.FAILURE.equals(tStatus))
        status = DequeueResult.DequeueStatus.FAILURE;
      else if (TDequeueStatus.RETRY.equals(tStatus))
        status = DequeueResult.DequeueStatus.RETRY;
      else {
        String message =
            "Internal Error: Received an unknown dequeue status of " + tStatus;
        Log.error(message);
        return new DequeueResult(DequeueResult.DequeueStatus.FAILURE, message);
      }
      return new DequeueResult(status, tDequeueResult.getMessage());
    }
  }
}
