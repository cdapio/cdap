/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Manages reading/writing of checkpoint information for a topic and partition.
 */
public final class CheckpointManager {

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(100);
  private static final byte [] OFFSET_COLNAME = Bytes.toBytes("nextOffset");
  private static final byte [] FILE_COLNAME = Bytes.toBytes("openedFiles");
  private static final byte [][] COLUMN_NAMES = new byte[][] {OFFSET_COLNAME, FILE_COLNAME};

  private final TransactionExecutor txExecutor;
  private final OrderedColumnarTable metaTable;
  private final byte [] rowKey;
  private final byte [] rowKeyPrefix;

  public CheckpointManager(OrderedColumnarTable metaTable,
                           TransactionSystemClient txClient,
                           String topic, int partition) {
    this.metaTable = metaTable;
    this.txExecutor = new DefaultTransactionExecutor(txClient, ImmutableList.of((TransactionAware) metaTable));
    this.rowKey = Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(topic), Bytes.toBytes(partition));
    this.rowKeyPrefix = Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(topic));
  }

  public CheckpointInfo getCheckpoint() throws Exception {
    return txExecutor.execute(new Callable<CheckpointInfo>() {
      @Override
      public CheckpointInfo call() throws Exception {
        OperationResult<Map<byte [], byte []>> result = metaTable.get(rowKey, COLUMN_NAMES);
        if (result.isEmpty() || result.getValue() == null || result.getValue().isEmpty()) {
          return null;
        }

        long offset = Bytes.toLong(result.getValue().get(OFFSET_COLNAME));
        Set<String> files = decodeStringSet(result.getValue().get(FILE_COLNAME));
        return new CheckpointInfo(offset, files);
      }
    });
  }

  public void saveCheckpoint(final int partition, final long nextOffset) throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        metaTable.put(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)), OFFSET_COLNAME, Bytes.toBytes(nextOffset));
      }
    });
  }

  public long getCheckpoint(final int partition) throws Exception {
    Long checkpoint = txExecutor.execute(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        byte [] result =
          metaTable.get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)), OFFSET_COLNAME);
        if (result == null) {
          return null;
        }

        return Bytes.toLong(result);
      }
    });

    return checkpoint == null ? -1 : checkpoint;
  }

  /**
   * Represents the information of a checkpoint.
   * TODO: delete this class
   */
  public static final class CheckpointInfo {
    private final long nextOffset;
    private final Set<String> files;

    public CheckpointInfo(long nextOffset, Set<String> files) {
      this.nextOffset = nextOffset;
      this.files = files;
    }

    public long getNextOffset() {
      return nextOffset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CheckpointInfo that = (CheckpointInfo) o;
      return nextOffset == that.nextOffset && !(files != null ? !files.equals(that.files) : that.files != null);

    }

    @Override
    public int hashCode() {
      int result = (int) (nextOffset ^ (nextOffset >>> 32));
      result = 31 * result + (files != null ? files.hashCode() : 0);
      return result;
    }
  }

  private Set<String> decodeStringSet(byte [] bytes) throws IOException {
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(bin, null);

    int size = decoder.readInt();
    Set<String> strings = Sets.newHashSetWithExpectedSize(size);
    while (size > 0) {
      for (int i = 0; i < size; ++i) {
        strings.add(decoder.readString());
      }
      size = decoder.readInt();
    }
    return strings;
  }
}
