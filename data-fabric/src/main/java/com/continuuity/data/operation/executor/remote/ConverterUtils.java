package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.OpenTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.TruncateTable;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.executor.remote.stubs.TClearFabric;
import com.continuuity.data.operation.executor.remote.stubs.TCompareAndSwap;
import com.continuuity.data.operation.executor.remote.stubs.TDelete;
import com.continuuity.data.operation.executor.remote.stubs.TDequeueResult;
import com.continuuity.data.operation.executor.remote.stubs.TDequeueStatus;
import com.continuuity.data.operation.executor.remote.stubs.TGetGroupId;
import com.continuuity.data.operation.executor.remote.stubs.TGetQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TGetSplits;
import com.continuuity.data.operation.executor.remote.stubs.TIncrement;
import com.continuuity.data.operation.executor.remote.stubs.TKeyRange;
import com.continuuity.data.operation.executor.remote.stubs.TOpenTable;
import com.continuuity.data.operation.executor.remote.stubs.TOperationContext;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinary;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryList;
import com.continuuity.data.operation.executor.remote.stubs.TOptionalBinaryMap;
import com.continuuity.data.operation.executor.remote.stubs.TQueueAck;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConfig;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConfigure;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConfigureGroups;
import com.continuuity.data.operation.executor.remote.stubs.TQueueConsumer;
import com.continuuity.data.operation.executor.remote.stubs.TQueueDequeue;
import com.continuuity.data.operation.executor.remote.stubs.TQueueDropInflight;
import com.continuuity.data.operation.executor.remote.stubs.TQueueEnqueue;
import com.continuuity.data.operation.executor.remote.stubs.TQueueEntry;
import com.continuuity.data.operation.executor.remote.stubs.TQueueEntryPointer;
import com.continuuity.data.operation.executor.remote.stubs.TQueueInfo;
import com.continuuity.data.operation.executor.remote.stubs.TQueuePartitioner;
import com.continuuity.data.operation.executor.remote.stubs.TQueueProducer;
import com.continuuity.data.operation.executor.remote.stubs.TQueueStateType;
import com.continuuity.data.operation.executor.remote.stubs.TRead;
import com.continuuity.data.operation.executor.remote.stubs.TReadAllKeys;
import com.continuuity.data.operation.executor.remote.stubs.TReadColumnRange;
import com.continuuity.data.operation.executor.remote.stubs.TReadPointer;
import com.continuuity.data.operation.executor.remote.stubs.TTransaction;
import com.continuuity.data.operation.executor.remote.stubs.TTransaction2;
import com.continuuity.data.operation.executor.remote.stubs.TTruncateTable;
import com.continuuity.data.operation.executor.remote.stubs.TWrite;
import com.continuuity.data.operation.executor.remote.stubs.TWriteOperation;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueueEntryPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigureGroups;
import com.continuuity.data.operation.ttqueue.admin.QueueDropInflight;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TBaseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility methods to convert to thrift and back.
 */
public class ConverterUtils {

  private static final Logger Log = LoggerFactory.getLogger(ConverterUtils.class);

  /**
   * wrap an operation context into a thrift object.
   */
  TOperationContext wrap(OperationContext context) {
    TOperationContext tcontext = new TOperationContext(context.getAccount());
    if (context.getApplication() != null) {
      tcontext.setApplication(context.getApplication());
    }
    return tcontext;
  }

  /**
   * unwrap an operation context.
   */
  OperationContext unwrap(TOperationContext tcontext) {
    return new OperationContext(tcontext.getAccount(), tcontext.getApplication());
  }

  /**
   * wrap an array of longs into a list of Long objects.
   */
  List<Long> wrap(long[] array) {
    List<Long> list = new ArrayList<Long>(array.length);
    for (long num : array) {
      list.add(num);
    }
    return list;
  }

  /**
   * unwrap an array of longs from a list of Long objects.
   */
  long[] unwrapAmounts(List<Long> list) {
    long[] longs = new long[list.size()];
    int i = 0;
    for (Long value : list) {
      longs[i++] = value;
    }
    return longs;
  }

  /**
   * wrap a byte array into a byte buffer.
   */
  ByteBuffer wrap(byte[] bytes) {
    if (bytes == null) {
      return null;
    } else {
      return ByteBuffer.wrap(bytes);
    }
  }

  /**
   * unwrap a byte array from a byte buffer.
   */
  byte[] unwrap(ByteBuffer buf) {
    if (buf == null) {
      return null;
    } else {
      return TBaseHelper.byteBufferToByteArray(buf);
    }
  }

  /**
   * wrap a byte array into an optional binary.
   */
  TOptionalBinary wrapBinary(byte[] bytes) {
    TOptionalBinary binary = new TOptionalBinary();
    if (bytes != null) {
      binary.setValue(bytes);
    }
    return binary;
  }

  /**
   * unwrap a byte array from an optional binary.
   */
  OperationResult<byte[]> unwrap(TOptionalBinary binary) {
    if (binary.isSetValue()) {
      return new OperationResult<byte[]>(binary.getValue());
    } else {
      return new OperationResult<byte[]>(binary.getStatus(), binary.getMessage());
    }
  }

  /**
   * wrap an array of byte arrays into a list of byte buffers.
   */
  List<ByteBuffer> wrap(byte[][] arrays) {
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(arrays.length);
    for (byte[] array : arrays) {
      buffers.add(wrap(array));
    }
    return buffers;
  }

  /**
   * wrap an list of byte arrays into a list of byte buffers.
   */
  List<ByteBuffer> wrap(List<byte[]> arrays) {
    if (arrays == null) {
      return null;
    }
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(arrays.size());
    for (byte[] array : arrays) {
      buffers.add(wrap(array));
    }
    return buffers;
  }

  /**
   * unwrap an array of byte arrays from a list of byte buffers.
   */
  byte[][] unwrap(List<ByteBuffer> buffers) {
    if (buffers == null) {
      return null;
    }
    byte[][] arrays = new byte[buffers.size()][];
    int i = 0;
    for (ByteBuffer buffer : buffers) {
      arrays[i++] = unwrap(buffer);
    }
    return arrays;
  }

  /**
   * unwrap an array of byte arrays from a list of byte buffers.
   */
  List<byte[]> unwrapList(List<ByteBuffer> buffers) {
    List<byte[]> arrays = new ArrayList<byte[]>(buffers.size());
    for (ByteBuffer buffer : buffers) {
      arrays.add(unwrap(buffer));
    }
    return arrays;
  }

  /**
   * wrap a map of byte arrays into an optional map of byte buffers.
   */
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

  /**
   * unwrap an optional map of byte buffers.
   */
  OperationResult<List<byte[]>> unwrap(TOptionalBinaryList opt) {
    if (opt.isSetTheList()) {
      return new OperationResult<List<byte[]>>(unwrapList(opt.getTheList()));
    } else {
      return new OperationResult<List<byte[]>>(opt.getStatus(), opt.getMessage());
    }
  }

  /**
   * wrap a map of byte arrays to long into a map of byte buffers to long.
   */
  Map<ByteBuffer, Long> wrapLongMap(Map<byte[], Long> map) {
    if (map == null) {
      return null;
    }
    Map<ByteBuffer, Long> result = Maps.newHashMap();
    for (Map.Entry<byte[], Long> entry : map.entrySet()) {
      result.put(wrap(entry.getKey()), entry.getValue());
    }
    return result;
  }

  /**
   * unwrap a map of byte arrays to long from a map of byte buffers to long.
   */
  Map<byte[], Long> unwrapLongMap(Map<ByteBuffer, Long> map) {
    Map<byte[], Long> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<ByteBuffer, Long> entry : map.entrySet()) {
      result.put(unwrap(entry.getKey()), entry.getValue());
    }
    return result;
  }

  /**
   * wrap a map of byte arrays into a map of byte buffers.
   */
  Map<ByteBuffer, TOptionalBinary> wrap(Map<byte[], byte[]> map) {
    if (map == null) {
      return null;
    }
    Map<ByteBuffer, TOptionalBinary> result = Maps.newHashMap();
    for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
      result.put(wrap(entry.getKey()), wrapBinary(entry.getValue()));
    }
    return result;
  }

  /**
   * unwrap a map of byte arrays from a map of byte buffers.
   */
  Map<byte[], byte[]> unwrap(Map<ByteBuffer, TOptionalBinary> map) {
    Map<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<ByteBuffer, TOptionalBinary> entry : map.entrySet()) {
      result.put(unwrap(entry.getKey()), unwrap(entry.getValue()).getValue());
    }
    return result;
  }

  /**
   * wrap a map of byte arrays into an optional map of byte buffers.
   */
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

  /**
   * unwrap an optional map of byte buffers.
   */
  OperationResult<Map<byte[], byte[]>> unwrap(TOptionalBinaryMap opt) {
    if (opt.isSetTheMap()) {
      return new OperationResult<Map<byte[], byte[]>>(unwrap(opt.getTheMap()));
    } else {
      return new OperationResult<Map<byte[], byte[]>>(opt.getStatus(), opt.getMessage());
    }
  }

  /**
   * wrap a key range.
   */
  TKeyRange wrap(KeyRange range) {
    TKeyRange tKeyRange = new TKeyRange();
    if (range.getStart() != null) {
      tKeyRange.setStart(wrap(range.getStart()));
    }
    if (range.getStop() != null) {
      tKeyRange.setStop(wrap(range.getStop()));
    }
    return tKeyRange;
  }

  /**
   * unwrap a key range.
   */
  KeyRange unwrap(TKeyRange tKeyRange) {
    return new KeyRange(tKeyRange.getStart(), tKeyRange.getStop());
  }

  /**
   * wrap a list of key ranges.
   */
  List<TKeyRange> wrap(OperationResult<List<KeyRange>> result) {
    if (result.isEmpty() || result.getValue() == null || result.getValue().isEmpty()) {
      return Collections.emptyList();
    } else {
      List<TKeyRange> tKeyRanges = Lists.newArrayListWithExpectedSize(result.getValue().size());
      for (KeyRange range : result.getValue()) {
        tKeyRanges.add(wrap(range));
      }
      return tKeyRanges;
    }
  }

  /**
   * unwrap a list of key ranges.
   */
  OperationResult<List<KeyRange>> unwrap(List<TKeyRange> tKeyRanges) {
    if (tKeyRanges.isEmpty()) {
      return new OperationResult<List<KeyRange>>(StatusCode.KEY_NOT_FOUND);
    } else {
      List<KeyRange> ranges = Lists.newArrayListWithExpectedSize(tKeyRanges.size());
      for (TKeyRange tKeyRange : tKeyRanges) {
        ranges.add(unwrap(tKeyRange));
      }
      return new OperationResult<List<KeyRange>>(ranges);
    }
  }

  /**
   * wrap a ClearFabric operation.
   */
  TClearFabric wrap(ClearFabric clearFabric) {
    TClearFabric tClearFabric = new TClearFabric(clearFabric.shouldClearData(), clearFabric.shouldClearMeta(),
                                                 clearFabric.shouldClearTables(), clearFabric.shouldClearQueues(),
                                                 clearFabric.shouldClearStreams(), clearFabric.getId());
    if (clearFabric.getMetricName() != null) {
      tClearFabric.setMetric(clearFabric.getMetricName());
    }
    return tClearFabric;
  }

  /**
   * unwrap a ClearFabric operation.
   */
  ClearFabric unwrap(TClearFabric tClearFabric) {
    ArrayList<ClearFabric.ToClear> toClear = Lists.newArrayList();
    if (tClearFabric.isClearData()) {
      toClear.add(ClearFabric.ToClear.DATA);
    }
    if (tClearFabric.isClearMeta()) {
      toClear.add(ClearFabric.ToClear.META);
    }
    if (tClearFabric.isClearTables()) {
      toClear.add(ClearFabric.ToClear.TABLES);
    }
    if (tClearFabric.isClearQueues()) {
      toClear.add(ClearFabric.ToClear.QUEUES);
    }
    if (tClearFabric.isClearStreams()) {
      toClear.add(ClearFabric.ToClear.STREAMS);
    }
    ClearFabric clearFabric = new ClearFabric(tClearFabric.getId(), toClear);
    if (tClearFabric.isSetMetric()) {
      clearFabric.setMetricName(tClearFabric.getMetric());
    }
    return clearFabric;
  }

  /**
   * wrap an OpenTable operation.
   */
  public TOpenTable wrap(OpenTable openTable) {
    TOpenTable tOpenTable = new TOpenTable(openTable.getTableName(), openTable.getId());
    if (openTable.getMetricName() != null) {
      tOpenTable.setMetric(openTable.getMetricName());
    }
    return tOpenTable;
  }

  /**
   * unwrap an OpenTable operation.
   */
  public OpenTable unwrap(TOpenTable tOpenTable) {
    OpenTable openTable = new OpenTable(tOpenTable.getId(), tOpenTable.getTable());
    if (tOpenTable.isSetMetric()) {
      openTable.setMetricName(tOpenTable.getMetric());
    }
    return openTable;
  }

  /**
   * wrap an TruncateTable operation.
   */
  public TTruncateTable wrap(TruncateTable truncateTable) {
    TTruncateTable tTruncateTable = new TTruncateTable(truncateTable.getTableName(), truncateTable.getId());
    if (truncateTable.getMetricName() != null) {
      tTruncateTable.setMetric(truncateTable.getMetricName());
    }
    return tTruncateTable;
  }

  /**
   * unwrap an TruncateTable operation.
   */
  public TruncateTable unwrap(TTruncateTable tTruncateTable) {
    TruncateTable truncateTable = new TruncateTable(tTruncateTable.getId(), tTruncateTable.getTable());
    if (tTruncateTable.isSetMetric()) {
      truncateTable.setMetricName(tTruncateTable.getMetric());
    }
    return truncateTable;
  }

  /**
   * wrap a Write operation.
   */
  TWrite wrap(Write write) {
    TWrite tWrite = new TWrite(wrap(write.getKey()), wrap(write.getColumns()), wrap(write.getValues()), write.getId());
    if (write.getTable() != null) {
      tWrite.setTable(write.getTable());
    }
    if (write.getMetricName() != null) {
      tWrite.setMetric(write.getMetricName());
    }
    return tWrite;
  }

  /**
   * unwrap a Write operation.
   */
  Write unwrap(TWrite tWrite) {
    Write write = new Write(tWrite.getId(), tWrite.isSetTable() ? tWrite.getTable() : null, tWrite.getKey(),
                            unwrap(tWrite.getColumns()), unwrap(tWrite.getValues()));
    if (tWrite.isSetMetric()) {
      write.setMetricName(tWrite.getMetric());
    }
    return write;
  }

  /**
   * wrap a Delete operation.
   */
  TDelete wrap(Delete delete) {
    TDelete tDelete = new TDelete(wrap(delete.getKey()), wrap(delete.getColumns()), delete.getId());
    if (delete.getTable() != null) {
      tDelete.setTable(delete.getTable());
    }
    if (delete.getMetricName() != null) {
      tDelete.setMetric(delete.getMetricName());
    }
    return tDelete;
  }

  /**
   * unwrap a Delete operation.
   */
  Delete unwrap(TDelete tDelete) {
    Delete delete = new Delete(tDelete.getId(), tDelete.isSetTable() ? tDelete.getTable() : null, tDelete.getKey(),
                               unwrap(tDelete.getColumns()));
    if (tDelete.isSetMetric()) {
      delete.setMetricName(tDelete.getMetric());
    }
    return delete;
  }

  /**
   * wrap an Increment operation.
   */
  TIncrement wrap(Increment increment) {
    TIncrement tIncrement = new TIncrement(wrap(increment.getKey()), wrap(increment.getColumns()),
                                           wrap(increment.getAmounts()), increment.getId());
    if (increment.getTable() != null) {
      tIncrement.setTable(increment.getTable());
    }
    if (increment.getMetricName() != null) {
      tIncrement.setMetric(increment.getMetricName());
    }
    return tIncrement;
  }

  /**
   * unwrap an Increment operation.
   */
  Increment unwrap(TIncrement tIncrement) {
    Increment increment = new Increment(tIncrement.getId(), tIncrement.isSetTable() ? tIncrement.getTable() : null,
                                        tIncrement.getKey(), unwrap(tIncrement.getColumns()),
                                        unwrapAmounts(tIncrement.getAmounts()));
    if (tIncrement.isSetMetric()) {
      increment.setMetricName(tIncrement.getMetric());
    }
    return increment;
  }

  /**
   * wrap a CompareAndSwap operation.
   */
  TCompareAndSwap wrap(CompareAndSwap compareAndSwap) {
    TCompareAndSwap tCompareAndSwap = new TCompareAndSwap(wrap(compareAndSwap.getKey()),
                                                          wrap(compareAndSwap.getColumn()),
                                                          wrap(compareAndSwap.getExpectedValue()),
                                                          wrap(compareAndSwap.getNewValue()), compareAndSwap.getId());
    if (compareAndSwap.getTable() != null) {
      tCompareAndSwap.setTable(compareAndSwap.getTable());
    }
    if (compareAndSwap.getMetricName() != null) {
      tCompareAndSwap.setMetric(compareAndSwap.getMetricName());
    }
    return tCompareAndSwap;
  }

  /**
   * unwrap a CompareAndSwap operation.
   */
  CompareAndSwap unwrap(TCompareAndSwap tCompareAndSwap) {
    CompareAndSwap compareAndSwap = new CompareAndSwap(tCompareAndSwap.getId(),
                                                       tCompareAndSwap.isSetTable() ? tCompareAndSwap.getTable() :
                                                         null, tCompareAndSwap.getKey(), tCompareAndSwap.getColumn(),
                                                       tCompareAndSwap.getExpectedValue(),
                                                       tCompareAndSwap.getNewValue());
    if (tCompareAndSwap.isSetMetric()) {
      compareAndSwap.setMetricName(tCompareAndSwap.getMetric());
    }
    return compareAndSwap;
  }

  /**
   * wrap a batch of write operations.
   */
  List<TWriteOperation> wrapBatch(List<WriteOperation> writes) throws TOperationException {
    List<TWriteOperation> tWrites = Lists.newArrayList();
    for (WriteOperation writeOp : writes) {
      if (Log.isTraceEnabled()) {
        Log.trace("  WriteOperation: " + writeOp.toString());
      }
      TWriteOperation tWriteOp = new TWriteOperation();
      if (writeOp instanceof Write) {
        tWriteOp.setWrite(wrap((Write) writeOp));
      } else if (writeOp instanceof Delete) {
        tWriteOp.setDelet(wrap((Delete) writeOp));
      } else if (writeOp instanceof Increment) {
        tWriteOp.setIncrement(wrap((Increment) writeOp));
      } else if (writeOp instanceof CompareAndSwap) {
        tWriteOp.setCompareAndSwap(wrap((CompareAndSwap) writeOp));
      } else if (writeOp instanceof QueueEnqueue) {
        tWriteOp.setQueueEnqueue(wrap((QueueEnqueue) writeOp));
      } else if (writeOp instanceof QueueAck) {
        tWriteOp.setQueueAck(wrap((QueueAck) writeOp));
      } else {
        Log.error("Internal Error: Received an unknown WriteOperation of class " + writeOp.getClass().getName() + ".");
        continue;
      }
      tWrites.add(tWriteOp);
    }
    return tWrites;
  }

  /**
   * unwrap a batch of write operations.
   */
  List<WriteOperation> unwrapBatch(List<TWriteOperation> batch) throws TOperationException {
    List<WriteOperation> writes = new ArrayList<WriteOperation>(batch.size());
    for (TWriteOperation tWriteOp : batch) {
      WriteOperation writeOp;
      if (tWriteOp.isSetWrite()) {
        writeOp = unwrap(tWriteOp.getWrite());
      } else if (tWriteOp.isSetDelet()) {
        writeOp = unwrap(tWriteOp.getDelet());
      } else if (tWriteOp.isSetIncrement()) {
        writeOp = unwrap(tWriteOp.getIncrement());
      } else if (tWriteOp.isSetCompareAndSwap()) {
        writeOp = unwrap(tWriteOp.getCompareAndSwap());
      } else if (tWriteOp.isSetQueueEnqueue()) {
        writeOp = unwrap(tWriteOp.getQueueEnqueue());
      } else if (tWriteOp.isSetQueueAck()) {
        writeOp = unwrap(tWriteOp.getQueueAck());
      } else {
        Log.error("Internal Error: Unkown TWriteOperation " + tWriteOp.toString() + " in batch. Skipping.");
        continue;
      }
      if (Log.isTraceEnabled()) {
        Log.trace("Operation in batch: " + writeOp);
      }
      writes.add(writeOp);
    }
    return writes;
  }

  /**
   * wrap a Read operation.
   */
  TRead wrap(Read read) {
    TRead tRead = new TRead(wrap(read.getKey()), wrap(read.getColumns()), read.getId());
    if (read.getTable() != null) {
      tRead.setTable(read.getTable());
    }
    if (read.getMetricName() != null) {
      tRead.setMetric(read.getMetricName());
    }
    return tRead;
  }

  /**
   * unwrap a Read operation.
   */
  Read unwrap(TRead tRead) {
    Read read = new Read(tRead.getId(), tRead.isSetTable() ? tRead.getTable() : null, tRead.getKey(),
                         unwrap(tRead.getColumns()));
    if (tRead.isSetMetric()) {
      read.setMetricName(tRead.getMetric());
    }
    return read;
  }

  /**
   * wrap a ReadAllKeys operation.
   */
  TReadAllKeys wrap(ReadAllKeys readAllKeys) {
    TReadAllKeys tReadAllKeys = new TReadAllKeys(readAllKeys.getOffset(), readAllKeys.getLimit(), readAllKeys.getId());
    if (readAllKeys.getTable() != null) {
      tReadAllKeys.setTable(readAllKeys.getTable());
    }
    if (readAllKeys.getMetricName() != null) {
      tReadAllKeys.setMetric(readAllKeys.getMetricName());
    }
    return tReadAllKeys;
  }

  /**
   * unwrap a ReadAllKeys operation.
   */
  ReadAllKeys unwrap(TReadAllKeys tReadAllKeys) {
    ReadAllKeys readAllKeys = new ReadAllKeys(tReadAllKeys.getId(), tReadAllKeys.isSetTable() ? tReadAllKeys.getTable
      () : null, tReadAllKeys.getOffset(), tReadAllKeys.getLimit());
    if (tReadAllKeys.isSetMetric()) {
      readAllKeys.setMetricName(tReadAllKeys.getMetric());
    }
    return readAllKeys;
  }

  /**
   * wrap a ReadColumnRange operation.
   */
  TReadColumnRange wrap(ReadColumnRange readColumnRange) {
    TReadColumnRange tReadColumnRange = new TReadColumnRange(wrap(readColumnRange.getKey()),
                                                             wrap(readColumnRange.getStartColumn()),
                                                             wrap(readColumnRange.getStopColumn()),
                                                             readColumnRange.getLimit(), readColumnRange.getId());
    if (readColumnRange.getTable() != null) {
      tReadColumnRange.setTable(readColumnRange.getTable());
    }
    if (readColumnRange.getMetricName() != null) {
      tReadColumnRange.setMetric(readColumnRange.getMetricName());
    }
    return tReadColumnRange;
  }

  /**
   * unwrap a ReadColumnRange operation.
   */
  ReadColumnRange unwrap(TReadColumnRange tReadColumnRange) {
    ReadColumnRange readColumnRange = new ReadColumnRange(tReadColumnRange.getId(),
                                                          tReadColumnRange.isSetTable() ? tReadColumnRange.getTable()
                                                            : null, tReadColumnRange.getKey(),
                                                          tReadColumnRange.getStartColumn(),
                                                          tReadColumnRange.getStopColumn(),
                                                          tReadColumnRange.getLimit());
    if (tReadColumnRange.isSetMetric()) {
      readColumnRange.setMetricName(tReadColumnRange.getMetric());
    }
    return readColumnRange;
  }

  /**
   * wrap a getSplits operations.
   */
  TGetSplits wrap(GetSplits getSplits) {
    TGetSplits tGetSplits = new TGetSplits(getSplits.getTable(), getSplits.getNumSplits(), getSplits.getId());
    if (getSplits.getStart() != null) {
      tGetSplits.setStart(getSplits.getStart());
    }
    if (getSplits.getStop() != null) {
      tGetSplits.setStop(getSplits.getStop());
    }
    if (getSplits.getColumns() != null) {
      tGetSplits.setColumns(wrap(getSplits.getColumns()));
    }
    if (getSplits.getMetricName() != null) {
      tGetSplits.setMetric(getSplits.getMetricName());
    }
    return tGetSplits;
  }

  /**
   * unwrap a getSplits operations.
   */
  GetSplits unwrap(TGetSplits tGetSplits) {
    GetSplits getSplits = new GetSplits(tGetSplits.getId(), tGetSplits.getTable(), tGetSplits.getNumSplits(),
                                        tGetSplits.getStart(), tGetSplits.getStop(), unwrap(tGetSplits.getColumns()));
    if (tGetSplits.isSetMetric()) {
      getSplits.setMetricName(tGetSplits.getMetric());
    }
    return getSplits;
  }

  /**
   * wrap a queue entry.
   */
  TQueueEntry wrap(QueueEntry entry) {
    TQueueEntry tQueueEntry = new TQueueEntry(wrap(entry.getData()));
    tQueueEntry.setHeader(entry.getHashKeys());
    return tQueueEntry;
  }

  /**
   * unwrap a queue entry.
   */
  QueueEntry unwrap(TQueueEntry entry) {
    return new QueueEntry(entry.getHeader(), entry.getData());
  }

  /**
   * wrap a batch of queue entries.
   */
  List<TQueueEntry> wrap(QueueEntry[] entries) {
    List<TQueueEntry> tQueueEntries = Lists.newArrayListWithCapacity(entries.length);
    for (QueueEntry entry : entries) {
      tQueueEntries.add(wrap(entry));
    }
    return tQueueEntries;
  }

  /**
   * unwrap a batch of queue entries.
   */
  QueueEntry[] unwrap(List<TQueueEntry> tEntries) {
    QueueEntry[] entries = new QueueEntry[tEntries.size()];
    int index = 0;
    for (TQueueEntry tEntry : tEntries) {
      entries[index++] = unwrap(tEntry);
    }
    return entries;
  }

  /**
   * wrap an Enqueue operation.
   */
  TQueueEnqueue wrap(QueueEnqueue enqueue) {
    TQueueEnqueue tQueueEnqueue = new TQueueEnqueue(wrap(enqueue.getKey()), wrap(enqueue.getEntries()),
                                                    enqueue.getId());
    if (enqueue.getProducer() != null) {
      tQueueEnqueue.setProducer(wrap(enqueue.getProducer()));
    }
    if (enqueue.getMetricName() != null) {
      tQueueEnqueue.setMetric(enqueue.getMetricName());
    }
    return tQueueEnqueue;
  }

  /**
   * unwrap an Enqueue operation.
   */
  QueueEnqueue unwrap(TQueueEnqueue tEnqueue) {
    QueueEnqueue enqueue = new QueueEnqueue(tEnqueue.getId(), unwrap(tEnqueue.getProducer()),
                                            tEnqueue.getQueueName(), unwrap(tEnqueue.getEntries()));
    if (tEnqueue.isSetMetric()) {
      enqueue.setMetricName(tEnqueue.getMetric());
    }
    return enqueue;
  }

  /**
   * wrap a DequeuePayload operation.
   */
  TQueueDequeue wrap(QueueDequeue dequeue) throws TOperationException {
    TQueueDequeue tQueueDequeue = new TQueueDequeue(wrap(dequeue.getKey()), wrap(dequeue.getConsumer()),
                                                    wrap(dequeue.getConfig()), dequeue.getId());
    if (dequeue.getMetricName() != null) {
      tQueueDequeue.setMetric(dequeue.getMetricName());
    }
    return tQueueDequeue;
  }

  /**
   * unwrap a DequeuePayload operation.
   */
  QueueDequeue unwrap(TQueueDequeue tDequeue) throws TOperationException {
    QueueDequeue dequeue = new QueueDequeue(tDequeue.getId(), tDequeue.getQueueName(),
                                            unwrap(tDequeue.getConsumer()), unwrap(tDequeue.getConfig()));
    if (tDequeue.isSetMetric()) {
      dequeue.setMetricName(tDequeue.getMetric());
    }
    return dequeue;
  }

  /**
   * wrap a QueueAck operation.
   */
  TQueueAck wrap(QueueAck ack) throws TOperationException {
    TQueueAck tAck = new TQueueAck(wrap(ack.getKey()), wrap(ack.getEntryPointers()), wrap(ack.getConsumer()),
                                   ack.getNumGroups(), ack.getId());
    if (ack.getMetricName() != null) {
      tAck.setMetric(ack.getMetricName());
    }
    return tAck;
  }

  /**
   * unwrap a QueueAck operation.
   */
  QueueAck unwrap(TQueueAck tQueueAck) throws TOperationException {
    QueueAck ack = new QueueAck(tQueueAck.getId(), tQueueAck.getQueueName(), unwrap(tQueueAck.getEntryPointers()),
                                unwrap(tQueueAck.getConsumer()), tQueueAck.getNumGroups());
    if (tQueueAck.isSetMetric()) {
      ack.setMetricName(tQueueAck.getMetric());
    }
    return ack;
  }

  /**
   * wrap a GetQueueInfo operation.
   */
  TGetQueueInfo wrap(GetQueueInfo getQueueInfo) {
    TGetQueueInfo tGetQueueInfo = new TGetQueueInfo(wrap(getQueueInfo.getQueueName()), getQueueInfo.getId());
    if (getQueueInfo.getMetricName() != null) {
      tGetQueueInfo.setMetric(getQueueInfo.getMetricName());
    }
    return tGetQueueInfo;
  }

  /**
   * unwrap a GetQueueInfo operation.
   */
  GetQueueInfo unwrap(TGetQueueInfo tGetQueueInfo) {
    GetQueueInfo getQueueInfo = new GetQueueInfo(tGetQueueInfo.getId(),
                                                                       tGetQueueInfo.getQueueName());
    if (tGetQueueInfo.isSetMetric()) {
      getQueueInfo.setMetricName(tGetQueueInfo.getMetric());
    }
    return getQueueInfo;
  }

  /**
   * wrap a GetGroupId operation.
   */
  TGetGroupId wrap(GetGroupID getGroupId) {
    TGetGroupId tGetGroupId = new TGetGroupId(wrap(getGroupId.getQueueName()));
    if (getGroupId.getMetricName() != null) {
      tGetGroupId.setMetric(getGroupId.getMetricName());
    }
    return tGetGroupId;
  }

  /**
   * unwrap a GetGroupId operation.
   */
  GetGroupID unwrap(TGetGroupId tGetGroupId) {
    GetGroupID getGroupID = new GetGroupID(tGetGroupId.getQueueName());
    if (tGetGroupId.isSetMetric()) {
      getGroupID.setMetricName(tGetGroupId.getMetric());
    }
    return getGroupID;
  }

  /**
   * wrap a queue entry pointer.
   */
  TQueueEntryPointer wrap(QueueEntryPointer entryPointer) {
    if (entryPointer == null) {
      return null;
    }
    return new TQueueEntryPointer(wrap(entryPointer.getQueueName()), entryPointer.getEntryId(),
                                  entryPointer.getShardId(), entryPointer.getTries());
  }

  /**
   * unwrap a queue entry pointer.
   */
  QueueEntryPointer unwrap(TQueueEntryPointer tPointer) {
    if (tPointer == null) {
      return null;
    }
    return new QueueEntryPointer(tPointer.getQueueName(), tPointer.getEntryId(), tPointer.getShardId(),
                                 tPointer.getTries());
  }

  /**
   * wrap a batch of queue entry pointers.
   */
  List<TQueueEntryPointer> wrap(QueueEntryPointer[] entryPointers) {
    List<TQueueEntryPointer> tPointers = Lists.newArrayListWithCapacity(entryPointers.length);
    for (QueueEntryPointer pointer : entryPointers) {
      tPointers.add(wrap(pointer));
    }
    return tPointers;
  }

  /**
   * wrap a batch of queue entry pointers.
   */
  QueueEntryPointer[] unwrap(List<TQueueEntryPointer> tPointers) {
    QueueEntryPointer[] pointers = new QueueEntryPointer[tPointers.size()];
    int index = 0;
    for (TQueueEntryPointer tPointer : tPointers) {
      pointers[index++] = unwrap(tPointer);
    }
    return pointers;
  }

  TQueueStateType wrap(QueueConsumer.StateType stateType) throws TOperationException {
    switch (stateType) {
      case INITIALIZED:
        return TQueueStateType.INITIALIZED;
      case UNINITIALIZED:
        return TQueueStateType.UNINITIALIZED;
      case NOT_FOUND:
        return TQueueStateType.NOT_FOUND;
    }
    throw new TOperationException(StatusCode.INTERNAL_ERROR, String.format("Unknown stateType %s", stateType));
  }

  QueueConsumer.StateType unwrap(TQueueStateType tQueueStateType) throws TOperationException {
    switch (tQueueStateType) {
      case INITIALIZED:
        return QueueConsumer.StateType.INITIALIZED;
      case UNINITIALIZED:
        return QueueConsumer.StateType.UNINITIALIZED;
      case NOT_FOUND:
        return QueueConsumer.StateType.NOT_FOUND;
    }
    throw new TOperationException(StatusCode.INTERNAL_ERROR, String.format("Unknown stateType %s", tQueueStateType));
  }

  /**
   * wrap a queue consumer.
   */
  TQueueConsumer wrap(QueueConsumer consumer) throws TOperationException {
    TQueueConsumer tQueueConsumer =  new TQueueConsumer(
        consumer.getInstanceId(),
        consumer.getGroupId(),
        consumer.getGroupSize(),
        consumer.isStateful(),
        wrap(consumer.getStateType()));
    if (consumer.getGroupName() != null) {
      tQueueConsumer.setGroupName(consumer.getGroupName());
    }
    if (consumer.getQueueConfig() != null) {
      tQueueConsumer.setQueueConfig(wrap(consumer.getQueueConfig()));
    }
    if (consumer.getPartitioningKey() != null) {
      tQueueConsumer.setPartitioningKey(consumer.getPartitioningKey());
    }
    // No need to serialize queue state since it is now stored in Opex
    return tQueueConsumer;
  }

  /**
   * unwrap a queue consumer.
   */
  QueueConsumer unwrap(TQueueConsumer tQueueConsumer) throws TOperationException {
    QueueConsumer consumer = new QueueConsumer(
      tQueueConsumer.getInstanceId(),
      tQueueConsumer.getGroupId(),
      tQueueConsumer.getGroupSize(),
      tQueueConsumer.isSetGroupName() ? tQueueConsumer.getGroupName() : null,
      tQueueConsumer.isSetPartitioningKey() ? tQueueConsumer.getPartitioningKey() : null,
      tQueueConsumer.isSetQueueConfig() ? unwrap(tQueueConsumer.getQueueConfig()) : null);
    consumer.setStateType(unwrap(tQueueConsumer.getStateType()));
    // No need to serialize queue state since it is now stored in Opex
    return consumer;
  }

  /**
   * wrap a queue producer.
   */
  TQueueProducer wrap(QueueProducer producer) {
    TQueueProducer tQueueProducer = new TQueueProducer();
    if (producer != null && producer.getProducerName() != null) {
      tQueueProducer.setName(producer.getProducerName());
    }
    return tQueueProducer;
  }

  /**
   * unwrap a queue producer.
   */
  QueueProducer unwrap(TQueueProducer tQueueProducer) {
    if (tQueueProducer == null) {
      return null;
    }
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
    if (PartitionerType.HASH.equals(partitioner)) {
      return TQueuePartitioner.HASH;
    }
    if (PartitionerType.FIFO.equals(partitioner)) {
      return TQueuePartitioner.FIFO;
    }
    if (PartitionerType.ROUND_ROBIN.equals(partitioner)) {
      return TQueuePartitioner.ROBIN;
    }
    Log.error("Internal Error: Received an unknown QueuePartitioner with " +
                "class " + partitioner + ". Defaulting to RANDOM.");
    return TQueuePartitioner.FIFO;
  }

  /**
   * unwrap a queue partitioner. We can only do this for the known
   * instances of the Thrift enum. If we encounter something else,
   * we log an error and fall back to random.
   */
  PartitionerType unwrap(TQueuePartitioner tPartitioner) {
    if (TQueuePartitioner.HASH.equals(tPartitioner)) {
      return PartitionerType.HASH;
    }
    if (TQueuePartitioner.FIFO.equals(tPartitioner)) {
      return PartitionerType.FIFO;
    }
    if (TQueuePartitioner.ROBIN.equals(tPartitioner)) {
      return PartitionerType.ROUND_ROBIN;
    }
    Log.error("Internal Error: Received unknown QueuePartitioner " +
                tPartitioner + ". Defaulting to " + PartitionerType.FIFO + ".");
    return PartitionerType.FIFO;
  }

  /**
   * wrap a queue config.
   */
  TQueueConfig wrap(QueueConfig config) {
    return new TQueueConfig(wrap(config.getPartitionerType()), config.isSingleEntry(), config.getBatchSize(),
                            config.returnsBatch());
  }

  /**
   * unwrap a queue config.
   */
  QueueConfig unwrap(TQueueConfig config) {
    if (config.getBatchSize() < 0) {
      return new QueueConfig(unwrap(config.getPartitioner()), config.isSingleEntry());
    } else {
      return new QueueConfig(unwrap(config.getPartitioner()), config.isSingleEntry(), config.getBatchSize(),
                             config.isReturnBatch());
    }
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

  /**
   * wrap a queue meta.
   */
  OperationResult<QueueInfo> unwrap(TQueueInfo tQueueInfo) {
    if (tQueueInfo.isEmpty()) {
      return new OperationResult<QueueInfo>(StatusCode.QUEUE_NOT_FOUND);
    } else {
      return new OperationResult<QueueInfo>(new QueueInfo(tQueueInfo.getJson()));
    }
  }

  /**
   * wrap a dequeue result.
   * If the status is unknown, return failure status and appropriate message.
   */
  TDequeueResult wrap(DequeueResult result, QueueConsumer consumer) throws TOperationException {
    TDequeueStatus status;
    if (DequeueResult.DequeueStatus.EMPTY.equals(result.getStatus())) {
      status = TDequeueStatus.EMPTY;
    } else if (DequeueResult.DequeueStatus.SUCCESS.equals(result.getStatus())) {
      status = TDequeueStatus.SUCCESS;
    } else {
      String message = "Internal Error: Received an unknown dequeue status of " + result.getStatus() + ".";
      Log.error(message);
      throw new TOperationException(StatusCode.INTERNAL_ERROR, message);
    }
    TDequeueResult tQueueResult = new TDequeueResult(status);
    if (result.getEntryPointers() != null) {
      tQueueResult.setPointers(wrap(result.getEntryPointers()));
    }
    if (result.getEntries() != null) {
      tQueueResult.setEntries(wrap(result.getEntries()));
    }
    if (consumer != null) {
      tQueueResult.setConsumer(wrap(consumer));
    }
    return tQueueResult;
  }

  /**
   * unwrap a dequeue result.
   * If the status is unknown, return failure status and appropriate message.
   */
  DequeueResult unwrap(TDequeueResult tDequeueResult, QueueConsumer consumer)
    throws OperationException, TOperationException {
    if (tDequeueResult.getConsumer() != null) {
      QueueConsumer retConsumer = unwrap(tDequeueResult.getConsumer());
      // No need to unwrap queue state, since it is now stored in Opex
      if (retConsumer != null) {
        consumer.setStateType(retConsumer.getStateType());
      }
    }
    if (tDequeueResult.getStatus().equals(TDequeueStatus.SUCCESS)) {
      return new DequeueResult(DequeueResult.DequeueStatus.SUCCESS, unwrap(tDequeueResult.getPointers()),
                               unwrap(tDequeueResult.getEntries()));
    } else {
      DequeueResult.DequeueStatus status;
      TDequeueStatus tStatus = tDequeueResult.getStatus();
      if (TDequeueStatus.EMPTY.equals(tStatus)) {
        status = DequeueResult.DequeueStatus.EMPTY;
      } else {
        String message = "Internal Error: Received an unknown dequeue status of " + tStatus;
        Log.error(message);
        throw new OperationException(StatusCode.INTERNAL_ERROR, message);
      }
      return new DequeueResult(status);
    }
  }

  TQueueConfigure wrap(QueueConfigure configure) throws TOperationException {
    if (configure == null) {
      return null;
    }
    TQueueConfigure tQueueConfigure =
      new TQueueConfigure(wrap(configure.getQueueName()), wrap(configure.getNewConsumer()));
    if (configure.getMetricName() != null) {
      tQueueConfigure.setMetric(configure.getMetricName());
    }
    return tQueueConfigure;
  }

  QueueConfigure unwrap(TQueueConfigure tQueueConfigure) throws TOperationException {
    if (tQueueConfigure == null) {
      return null;
    }
    QueueConfigure queueConfigure =
      new QueueConfigure(tQueueConfigure.getQueueName(),
                         unwrap(tQueueConfigure.getNewConsumer()));
    if (tQueueConfigure.getMetric() != null) {
      queueConfigure.setMetricName(tQueueConfigure.getMetric());
    }
    return queueConfigure;
  }
  
  TQueueConfigureGroups wrap(QueueConfigureGroups configure) throws TOperationException {
    if (configure == null) {
      return null;
    }
    TQueueConfigureGroups tQueueConfigureGroups = new TQueueConfigureGroups(wrap(configure.getQueueName()), 
                                                                            configure.getGroupIds());
    if (configure.getMetricName() != null) {
      tQueueConfigureGroups.setMetric(configure.getMetricName());
    }
    return tQueueConfigureGroups;
  }

  QueueConfigureGroups unwrap(TQueueConfigureGroups tQueueConfigureGroups) throws TOperationException {
    if (tQueueConfigureGroups == null) {
      return null;
    }
    QueueConfigureGroups configure = new QueueConfigureGroups(tQueueConfigureGroups.getQueueName(),
                                                              tQueueConfigureGroups.getGroupIds());
    if (tQueueConfigureGroups.getMetric() != null) {
      configure.setMetricName(tQueueConfigureGroups.getMetric());
    }
    return configure;
  }

  TQueueDropInflight wrap(QueueDropInflight op) throws TOperationException {
    if (op == null) {
      return null;
    }
    TQueueDropInflight tOp =
      new TQueueDropInflight(wrap(op.getQueueName()),
                          wrap(op.getConsumer()));
    if (op.getMetricName() != null) {
      tOp.setMetric(op.getMetricName());
    }
    return tOp;
  }

  QueueDropInflight unwrap(TQueueDropInflight tOp) throws TOperationException {
    if (tOp == null) {
      return null;
    }
    QueueDropInflight op =
      new QueueDropInflight(tOp.getQueueName(),
                         unwrap(tOp.getConsumer()));
    if (tOp.getMetric() != null) {
      op.setMetricName(tOp.getMetric());
    }
    return op;
  }

  /**
   * wrap a read pointer. Only memory read pionters are supported.
   */
  TReadPointer wrap(ReadPointer readPointer) throws TOperationException {
    if (!(readPointer instanceof MemoryReadPointer)) {
      String message = String.format("Unsupported readPointer implementation %s, only MemortReadPointer is supported",
                                     readPointer.getClass().getName());
      Log.error(message);
      throw new TOperationException(StatusCode.INTERNAL_ERROR, message);

    }
    MemoryReadPointer rp = (MemoryReadPointer) readPointer;
    return new TReadPointer(rp.getWriteVersion(), rp.getReadPointer(), rp.getReadExcludes());
  }

  /**
   * unwrap a read pointer.
   */
  ReadPointer unwrap(TReadPointer trp) {
    return new MemoryReadPointer(trp.getWritePoint(), trp.getReadPoint(), trp.getExcludes());
  }

  /**
   * wrap a transaction.
   */
  TTransaction wrap(Transaction tx) throws TOperationException {
    if (tx == null) {
      return new TTransaction(true);
    }
    TTransaction ttx = new TTransaction(false);
    ttx.setReadPointer(wrap(tx.getReadPointer()));
    ttx.setTxid(tx.getWriteVersion());
    ttx.setTrackChanges(tx.isTrackChanges());
    return ttx;
  }

  /**
   * unwrap a transaction.
   */
  Transaction unwrap(TTransaction ttx) {
    return ttx.isIsNull() ? null : new Transaction(ttx.getTxid(), unwrap(ttx.getReadPointer()), ttx.isTrackChanges());
  }

  /**
   * wrap an operation exception.
   */
  TOperationException wrap(OperationException e) {
    return new TOperationException(e.getStatus(), e.getMessage());
  }

  /**
   * unwrap an operation exception.
   */
  OperationException unwrap(TOperationException te) {
    return new OperationException(te.getStatus(), te.getMessage());
  }

  // temporary TxDs2 stuff

  TTransaction2 wrap(com.continuuity.data2.transaction.Transaction tx) {
    List<Long> invalids = Lists.newArrayList();
    for (long txid : tx.getInvalids()) {
      invalids.add(txid);
    }
    List<Long> inProgress = Lists.newArrayList();
    for (long txid : tx.getInProgress()) {
      inProgress.add(txid);
    }
    return new TTransaction2(tx.getWritePointer(), tx.getReadPointer(),
                             invalids, inProgress, tx.getFirstShortInProgress());
  }

  com.continuuity.data2.transaction.Transaction unwrap(TTransaction2 tx) {
    long[] invalids = new long[tx.invalids.size()];
    int i = 0;
    for (Long txid : tx.invalids) {
      invalids[i++] = txid;
    }
    long[] inProgress = new long[tx.inProgress.size()];
    i = 0;
    for (Long txid : tx.inProgress) {
      inProgress[i++] = txid;
    }
    return new com.continuuity.data2.transaction.Transaction(tx.readPointer, tx.writePointer,
                                                             invalids, inProgress, tx.getFirstShort());
  }
}
