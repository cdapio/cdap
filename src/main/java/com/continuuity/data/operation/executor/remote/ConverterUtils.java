package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.remote.stubs.TClearFabric;
import com.continuuity.data.operation.executor.remote.stubs.TCompareAndSwap;
import com.continuuity.data.operation.executor.remote.stubs.TDelete;
import com.continuuity.data.operation.executor.remote.stubs.TDequeueResult;
import com.continuuity.data.operation.executor.remote.stubs.TDequeueStatus;
import com.continuuity.data.operation.executor.remote.stubs.TGetGroupId;
import com.continuuity.data.operation.executor.remote.stubs.TGetQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TIncrement;
import com.continuuity.data.operation.executor.remote.stubs.TOpenTable;
import com.continuuity.data.operation.executor.remote.stubs.TOperationContext;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinary;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryList;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryMap;
import com.continuuity.data.operation.executor.remote.stubs.TQueueAck;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConfig;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConsumer;
import com.continuuity.data.operation.executor.remote.stubs.TQueueDequeue;
import com.continuuity.data.operation.executor.remote.stubs.TQueueEnqueue;
import com.continuuity.data.operation.executor.remote.stubs.TQueueEntryPointer;
import com.continuuity.data.operation.executor.remote.stubs.TQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TQueuePartitioner;
import com.continuuity.data.operation.executor.remote.stubs.TQueueProducer;
import com.continuuity.data.operation.executor.remote.stubs.TRead;
import com.continuuity.data.operation.executor.remote.stubs.TReadAllKeys;
import com.continuuity.data.operation.executor.remote.stubs.TReadColumnRange;
import com.continuuity.data.operation.executor.remote.stubs.TWrite;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueAdmin;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TBaseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.continuuity.data.operation.ttqueue.QueueAdmin.QueueInfo;

public class ConverterUtils {

  private static final Logger Log =
      LoggerFactory.getLogger(ConverterUtils.class);

  /** wrap an operation context into a thrift object */
  TOperationContext wrap(OperationContext context) {
    TOperationContext tcontext = new TOperationContext(context.getAccount());
    if (context.getApplication() != null) tcontext.setApplication(context
        .getApplication());
    return tcontext;
  }

  /** unwrap an operation context */
  OperationContext unwrap(TOperationContext tcontext) {
    return new OperationContext(
        tcontext.getAccount(), tcontext.getApplication());
  }

  /** wrap an array of longs into a list of Long objects */
  List<Long> wrap(long[] array) {
    List<Long> list = new ArrayList<Long>(array.length);
    for (long num : array) list.add(num);
    return list;
  }
  /** unwrap an array of longs from a list of Long objects */
  long[] unwrapAmounts(List<Long> list) {
    long[] longs = new long[list.size()];
    int i = 0;
    for (Long value : list)
      longs[i++] = value;
    return longs;
  }

  private ByteBuffer wrap(Map<String,String> map) {
    byte[] mapAsBytes;
    if (map == null)
      return null;
    else {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      try {
        encoder.writeInt(map.size());
        for(Map.Entry<String,String> entry: map.entrySet()) {
          encoder.writeString(entry.getKey());
          encoder.writeString(entry.getValue());
        }
        mapAsBytes=bos.toByteArray();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
      return ByteBuffer.wrap(mapAsBytes);
    }
  }

  Map<String,String> unwrap(byte[] mapAsBytes) {
    Map<String,String> map=null;
    if (mapAsBytes == null) return map;
    else {
      ByteArrayInputStream bis = new ByteArrayInputStream(mapAsBytes);
      Decoder decoder = new BinaryDecoder(bis);
      int size;
      try {
        size = decoder.readInt();
        if (size>0) {
          map=Maps.newHashMap();
          for(int i=0; i<size; i++) {
            map.put(decoder.readString(),decoder.readString());
          }
        }
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return null;
      }
      return map;
    }
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
  TOptionalBinary wrapBinary(byte[] bytes) {
    TOptionalBinary binary = new TOptionalBinary();
    if (bytes != null) {
      binary.setValue(bytes);
    }
    return binary;
  }
  /** unwrap a byte array from an optional binary */
  OperationResult<byte[]> unwrap(TOptionalBinary binary) {
    if (binary.isSetValue()) {
      return new OperationResult<byte[]>(binary.getValue());
    } else {
      return new OperationResult<byte[]>(binary.getStatus(),
          binary.getMessage());
    }
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
  TOptionalBinaryList wrapList(OperationResult<List<byte[]>> result) {
    TOptionalBinaryList opt = new TOptionalBinaryList();
    if (result.isEmpty()) {
      opt.setStatus(result.getStatus());
      opt.setMessage(result.getMessage());
    } else {
      opt.setTheList(wrap(result.getValue()));
    }
    return opt;
  }
  /** unwrap an optional map of byte buffers */
  OperationResult<List<byte[]>> unwrap(TOptionalBinaryList opt) {
    if (opt.isSetTheList()) {
      return new OperationResult<List<byte[]>>(unwrapList(opt.getTheList()));
    } else {
      return new OperationResult<List<byte[]>>(opt.getStatus(),
          opt.getMessage());
    }
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
    Map<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<ByteBuffer, TOptionalBinary> entry : map.entrySet())
      result.put(unwrap(entry.getKey()), unwrap(entry.getValue()).getValue());
    return result;
  }

  /** wrap a map of byte arrays into an optional map of byte buffers */
  TOptionalBinaryMap wrapMap(OperationResult<Map<byte[], byte[]>> result) {
    TOptionalBinaryMap opt = new TOptionalBinaryMap();
    if (result.isEmpty()) {
      opt.setStatus(result.getStatus());
      opt.setMessage(result.getMessage());
    } else {
      opt.setTheMap(wrap(result.getValue()));
    }
    return opt;
  }
  /** unwrap an optional map of byte buffers */
  OperationResult<Map<byte[], byte[]>> unwrap(TOptionalBinaryMap opt) {
    if (opt.isSetTheMap()) {
      return new OperationResult<Map<byte[], byte[]>>(unwrap(opt.getTheMap()));
    } else {
      return new OperationResult<Map<byte[], byte[]>>(
          opt.getStatus(), opt.getMessage());
    }
  }

  /** wrap a ClearFabric operation */
  TClearFabric wrap(ClearFabric clearFabric) {
    return new TClearFabric(
        clearFabric.shouldClearData(),
        clearFabric.shouldClearMeta(),
        clearFabric.shouldClearTables(),
        clearFabric.shouldClearQueues(),
        clearFabric.shouldClearStreams(),
        clearFabric.getId());
  }
  /** unwrap a ClearFabric operation */
  ClearFabric unwrap(TClearFabric tClearFabric) {
    ArrayList<ClearFabric.ToClear> toClear = Lists.newArrayList();
    if (tClearFabric.isClearData()) toClear.add(ClearFabric.ToClear.DATA);
    if (tClearFabric.isClearMeta()) toClear.add(ClearFabric.ToClear.META);
    if (tClearFabric.isClearTables()) toClear.add(ClearFabric.ToClear.TABLES);
    if (tClearFabric.isClearQueues()) toClear.add(ClearFabric.ToClear.QUEUES);
    if (tClearFabric.isClearStreams()) toClear.add(ClearFabric.ToClear.STREAMS);
    return new ClearFabric(tClearFabric.getId(), toClear);
  }

  /** wrap an OpenTable operation */
  public TOpenTable wrap(OpenTable openTable) {
    return new TOpenTable(openTable.getTableName(), openTable.getId());
  }
  /** unwrap an OpenTable operation */
  public OpenTable unwrap(TOpenTable tOpenTable) {
    return new OpenTable(tOpenTable.getTable());
  }

  /** wrap a Write operation */
  TWrite wrap(Write write) {
    TWrite tWrite = new TWrite(
        wrap(write.getKey()),
        wrap(write.getColumns()),
        wrap(write.getValues()),
        write.getId());
    if (write.getTable() != null)
      tWrite.setTable(write.getTable());
    return tWrite;
  }
  /** unwrap a Write operation */
  Write unwrap(TWrite tWrite) {
    return new Write(
        tWrite.getId(),
        tWrite.isSetTable() ? tWrite.getTable() : null,
        tWrite.getKey(),
        unwrap(tWrite.getColumns()),
        unwrap(tWrite.getValues()));
  }

  /** wrap a Delete operation */
  TDelete wrap(Delete delete) {
    TDelete tDelete = new TDelete(
        wrap(delete.getKey()),
        wrap(delete.getColumns()),
        delete.getId());
    if (delete.getTable() != null)
      tDelete.setTable(delete.getTable());
    return tDelete;
  }
  /** unwrap a Delete operation */
  Delete unwrap(TDelete tDelete) {
    return new Delete(
        tDelete.getId(),
        tDelete.isSetTable() ? tDelete.getTable() : null,
        tDelete.getKey(),
        unwrap(tDelete.getColumns()));
  }

  /** wrap an Increment operation */
  TIncrement wrap(Increment increment) {
    TIncrement tIncrement = new TIncrement(
        wrap(increment.getKey()),
        wrap(increment.getColumns()),
        wrap(increment.getAmounts()),
        increment.getId());
    if (increment.getTable() != null)
      tIncrement.setTable(increment.getTable());
    return tIncrement;
  }
  /** unwrap an Increment operation */
  Increment unwrap(TIncrement tIncrement) {
    return new Increment(
        tIncrement.getId(),
        tIncrement.isSetTable() ? tIncrement.getTable() : null,
        tIncrement.getKey(),
        unwrap(tIncrement.getColumns()),
        unwrapAmounts(tIncrement.getAmounts()));
  }

  /** wrap a CompareAndSwap operation */
  TCompareAndSwap wrap(CompareAndSwap compareAndSwap) {
    TCompareAndSwap tCompareAndSwap = new TCompareAndSwap(
        wrap(compareAndSwap.getKey()),
        wrap(compareAndSwap.getColumn()),
        wrap(compareAndSwap.getExpectedValue()),
        wrap(compareAndSwap.getNewValue()),
        compareAndSwap.getId());
    if (compareAndSwap.getTable() != null)
      tCompareAndSwap.setTable(compareAndSwap.getTable());
    return tCompareAndSwap;
  }
  /** unwrap a CompareAndSwap operation */
  CompareAndSwap unwrap(TCompareAndSwap tCompareAndSwap) {
    return new CompareAndSwap(
        tCompareAndSwap.getId(),
        tCompareAndSwap.isSetTable() ? tCompareAndSwap.getTable() : null,
        tCompareAndSwap.getKey(),
        tCompareAndSwap.getColumn(),
        tCompareAndSwap.getExpectedValue(),
        tCompareAndSwap.getNewValue());
  }

  /** wrap a Read operation */
  TRead wrap(Read read) {
    TRead tRead = new TRead(
        wrap(read.getKey()),
        wrap(read.getColumns()),
        read.getId());
    if (read.getTable() != null)
      tRead.setTable(read.getTable());
    return tRead;
  }
  /** unwrap a Read operation */
  Read unwrap(TRead tRead) {
    return new Read(
        tRead.getId(),
        tRead.isSetTable() ? tRead.getTable() : null,
        tRead.getKey(),
        unwrap(tRead.getColumns()));
  }

  /** wrap a ReadAllKeys operation */
  TReadAllKeys wrap(ReadAllKeys readAllKeys) {
    TReadAllKeys tReadAllKeys = new TReadAllKeys(
        readAllKeys.getOffset(),
        readAllKeys.getLimit(),
        readAllKeys.getId());
    if (readAllKeys.getTable() != null)
      tReadAllKeys.setTable(readAllKeys.getTable());
    return tReadAllKeys;
  }
  /** unwrap a ReadAllKeys operation */
  ReadAllKeys unwrap(TReadAllKeys tReadAllKeys) {
    return new ReadAllKeys(
        tReadAllKeys.getId(),
        tReadAllKeys.isSetTable() ? tReadAllKeys.getTable() : null,
        tReadAllKeys.getOffset(),
        tReadAllKeys.getLimit());
  }

  /** wrap a ReadColumnRange operation */
  TReadColumnRange wrap(ReadColumnRange readColumnRange) {
    TReadColumnRange tReadColumnRange = new TReadColumnRange(
        wrap(readColumnRange.getKey()),
        wrap(readColumnRange.getStartColumn()),
        wrap(readColumnRange.getStopColumn()),
        readColumnRange.getLimit(),
        readColumnRange.getId());
    if (readColumnRange.getTable() != null)
      tReadColumnRange.setTable(readColumnRange.getTable());
    return tReadColumnRange;
  }
  /** unwrap a ReadColumnRange operation */
  ReadColumnRange unwrap(TReadColumnRange tReadColumnRange) {
    return new ReadColumnRange(
        tReadColumnRange.getId(),
        tReadColumnRange.isSetTable() ? tReadColumnRange.getTable() : null,
        tReadColumnRange.getKey(),
        tReadColumnRange.getStartColumn(),
        tReadColumnRange.getStopColumn(),
        tReadColumnRange.getLimit());
  }

  /** wrap an EnqueuePayload operation */
  TQueueEnqueue wrap(QueueEnqueue enqueue) {
    TQueueEnqueue tQueueEnqueue = new TQueueEnqueue(
        wrap(enqueue.getKey()),
        wrap(enqueue.getData()),
        enqueue.getHeaderVersion(),
        wrap(enqueue.getHeaders()),
        enqueue.getId());
    if (enqueue.getProducer() != null)
      tQueueEnqueue.setProducer(wrap(enqueue.getProducer()));
    return tQueueEnqueue;
  }
  /** unwrap an EnqueuePayload operation */
  QueueEnqueue unwrap(TQueueEnqueue tEnqueue) {
    return new QueueEnqueue(
        tEnqueue.getId(),
        unwrap(tEnqueue.getProducer()),
        tEnqueue.getQueueName(),
        unwrap(tEnqueue.getHeaders()),
        tEnqueue.getValue());
  }

  /** wrap a DequeuePayload operation */
  TQueueDequeue wrap(QueueDequeue dequeue) {
    return new TQueueDequeue(
        wrap(dequeue.getKey()),
        wrap(dequeue.getConsumer()),
        wrap(dequeue.getConfig()),
        dequeue.getId());
  }
  /** unwrap a DequeuePayload operation */
  QueueDequeue unwrap(TQueueDequeue tDequeue) {
    return new QueueDequeue(
        tDequeue.getId(),
        tDequeue.getQueueName(),
        unwrap(tDequeue.getConsumer()),
        unwrap(tDequeue.getConfig()));
  }

  /** wrap a QueueAck operation */
  TQueueAck wrap(QueueAck ack) {
    return new TQueueAck(
        wrap(ack.getKey()),
        wrap(ack.getEntryPointer()),
        wrap(ack.getConsumer()),
        ack.getNumGroups(),
        ack.getId());
  }
  /** unwrap a QueueAck operation */
  QueueAck unwrap(TQueueAck tQueueAck) {
    return new QueueAck(
        tQueueAck.getId(),
        tQueueAck.getQueueName(),
        unwrap(tQueueAck.getEntryPointer()),
        unwrap(tQueueAck.getConsumer()),
        tQueueAck.getNumGroups());
  }

  /** wrap a GetQueueInfo operation */
  TGetQueueInfo wrap(QueueAdmin.GetQueueInfo getQueueInfo) {
    return new TGetQueueInfo(
        wrap(getQueueInfo.getQueueName()),
        getQueueInfo.getId());
  }
  /** unwrap a GetQueueInfo operation */
  QueueAdmin.GetQueueInfo unwrap(TGetQueueInfo tGetQueueInfo) {
    return new QueueAdmin.GetQueueInfo(
        tGetQueueInfo.getId(),
        tGetQueueInfo.getQueueName());
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
    if (entryPointer == null)
      return null;
    return new TQueueEntryPointer(
        wrap(entryPointer.getQueueName()),
        entryPointer.getEntryId(),
        entryPointer.getShardId());
  }
  /** unwrap a queue entry pointer */
  QueueEntryPointer unwrap(TQueueEntryPointer tPointer) {
    if (tPointer == null)
      return null;
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
    TQueueConsumer tQueueConsumer=  new TQueueConsumer(
        consumer.getInstanceId(),
        consumer.getGroupId(),
        consumer.getGroupSize());
    if (consumer.getGroupName() != null)
      tQueueConsumer.setGroupName(consumer.getGroupName());
    if (consumer.getQueueConfig() != null)
      tQueueConsumer.setQueueConfig(wrap(consumer.getQueueConfig()));
    return tQueueConsumer;
  }
  /** unwrap a queue consumer */
  QueueConsumer unwrap(TQueueConsumer tQueueConsumer) {
    return new QueueConsumer(
        tQueueConsumer.getInstanceId(),
        tQueueConsumer.getGroupId(),
        tQueueConsumer.getGroupSize(),
        tQueueConsumer.isSetGroupName() ? tQueueConsumer.getGroupName() : null,
        tQueueConsumer.isSetQueueConfig() ? unwrap(tQueueConsumer.getQueueConfig()) : null);
  }

  /** wrap a queue producer */
  TQueueProducer wrap(QueueProducer producer) {
    TQueueProducer tQueueProducer = new TQueueProducer();
    if (producer != null && producer.getProducerName() != null)
      tQueueProducer.setName(producer.getProducerName());
    return tQueueProducer;
  }
  /** unwrap a queue producer */
  QueueProducer unwrap(TQueueProducer tQueueProducer) {
    if (tQueueProducer == null) return null;
    return new QueueProducer(tQueueProducer.getName());
  }

  /**
   * wrap a queue partitioner. This can be a little tricky: in
   * Thrift, this is an enum, whereas in data fabric, this is
   * an actual class (it could be custom implementation by the
   * caller). If we find a partitioner that is not predefined
   * by data fabric, we log an error and default to random.
   */
  TQueuePartitioner wrap(PartitionerType partitioner) {
    if (PartitionerType.HASH_ON_VALUE.equals(partitioner))
      return TQueuePartitioner.HASH;
    if (PartitionerType.RANDOM.equals(partitioner))
      return TQueuePartitioner.RANDOM;
    if (PartitionerType.MODULO_LONG_VALUE.equals(partitioner))
      return TQueuePartitioner.LONGMOD;
    Log.error("Internal Error: Received an unknown QueuePartitioner with " +
        "class " + partitioner + ". Defaulting to RANDOM.");
    return TQueuePartitioner.RANDOM;
  }
  /**
   * unwrap a queue partitioner. We can only do this for the known
   * instances of the Thrift enum. If we encounter something else,
   * we log an error and fall back to random.
   */
  PartitionerType unwrap(TQueuePartitioner tPartitioner) {
    if (TQueuePartitioner.HASH.equals(tPartitioner))
      return PartitionerType.HASH_ON_VALUE;
    if (TQueuePartitioner.RANDOM.equals(tPartitioner))
      return PartitionerType.RANDOM;
    if (TQueuePartitioner.LONGMOD.equals(tPartitioner))
      return PartitionerType.MODULO_LONG_VALUE;
    Log.error("Internal Error: Received unknown QueuePartitioner " +
        tPartitioner + ". Defaulting to " + PartitionerType.RANDOM + ".");
    return PartitionerType.RANDOM;
  }

  /** wrap a queue config */
  TQueueConfig wrap(QueueConfig config) {
    return new TQueueConfig(
        wrap(config.getPartitionerType()),
        config.isSingleEntry());
  }
  /** unwrap a queue config */
  QueueConfig unwrap(TQueueConfig config) {
    return new QueueConfig(
        unwrap(config.getPartitioner()),
        config.isSingleEntry());
  }

  /**
   * wrap a queue meta.
   * The first field of TQueueMeta indicates whether this represents null
   */
  TQueueInfo wrap(OperationResult<QueueInfo> info) {
    if (info.isEmpty()) {
      return new TQueueInfo(true);
    } else {
      return new TQueueInfo(false).
          setJson(info.getValue().getJSONString());
    }
  }
  /** wrap a queue meta */
  OperationResult<QueueInfo> unwrap(TQueueInfo tQueueInfo) {
    if (tQueueInfo.isEmpty()) {
      return new OperationResult<QueueInfo>(StatusCode.QUEUE_NOT_FOUND);
    } else {
      return new OperationResult<QueueInfo>(
          new QueueInfo(tQueueInfo.getJson()));
    }
  }

  /**
   * wrap a dequeue result.
   * If the status is unknown, return failure status and appropriate message.
   */
  TDequeueResult wrap(DequeueResult result) throws TOperationException {
    TDequeueStatus status;
    if (DequeueResult.DequeueStatus.EMPTY.equals(result.getStatus()))
      status = TDequeueStatus.EMPTY;
    else if (DequeueResult.DequeueStatus.RETRY.equals(result.getStatus()))
      status = TDequeueStatus.RETRY;
    else if (DequeueResult.DequeueStatus.SUCCESS.equals(result.getStatus()))
      status = TDequeueStatus.SUCCESS;
    else {
      String message = "Internal Error: Received an unknown dequeue status of "
          + result.getStatus() + ".";
      Log.error(message);
      throw new TOperationException(StatusCode.INTERNAL_ERROR, message);
    }
    return new TDequeueResult(status,
        wrap(result.getEntryPointer()),
        wrap(result.getValue()));
  }
  /**
   * unwrap a dequeue result
   * If the status is unknown, return failure status and appropriate message.
   */
  DequeueResult unwrap(TDequeueResult tDequeueResult)
      throws OperationException {
    if (tDequeueResult.getStatus().equals(TDequeueStatus.SUCCESS)) {
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS,
          unwrap(tDequeueResult.getPointer()),
          tDequeueResult.getValue());
    } else {
      DequeueResult.DequeueStatus status;
      TDequeueStatus tStatus = tDequeueResult.getStatus();
      if (TDequeueStatus.EMPTY.equals(tStatus))
        status = DequeueResult.DequeueStatus.EMPTY;
      else if (TDequeueStatus.RETRY.equals(tStatus))
        status = DequeueResult.DequeueStatus.RETRY;
      else {
        String message =
            "Internal Error: Received an unknown dequeue status of " + tStatus;
        Log.error(message);
        throw new OperationException(StatusCode.INTERNAL_ERROR, message);
      }
      return new DequeueResult(status);
    }
  }

  /**
   * wrap an operation exception
   */
  TOperationException wrap(OperationException e) {
    return new TOperationException(e.getStatus(), e.getMessage());
  }

  /**
   * unwrap an operation exception
   */
  OperationException unwrap(TOperationException te) {
    return new OperationException(te.getStatus(), te.getMessage());
  }
}
