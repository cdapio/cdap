package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TTransaction2;
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

/**
 * Utility methods to convert to thrift and back.
 */
public class ConverterUtils {

  private static final Logger Log = LoggerFactory.getLogger(ConverterUtils.class);

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
