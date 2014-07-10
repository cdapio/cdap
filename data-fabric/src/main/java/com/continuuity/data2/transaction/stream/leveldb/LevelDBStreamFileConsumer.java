/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.leveldb;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data.stream.StreamEventOffset;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.lib.table.leveldb.KeyValue;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.stream.AbstractStreamFileConsumer;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumerState;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 *
 */
public final class LevelDBStreamFileConsumer extends AbstractStreamFileConsumer {

  // used for undoing state. The value does not matter, but the OcTable interface was written to expect some value.
  private static final byte[] DUMMY_STATE_CONTENT = { };

  private final LevelDBOcTableCore tableCore;
  private final Object dbLock;
  private final NavigableMap<byte[], NavigableMap<byte[], byte[]>> rowMapForClaim;
  private final NavigableMap<byte[], byte[]> colMapForClaim;


  /**
   * @param streamConfig   Stream configuration.
   * @param consumerConfig Consumer configuration.
   * @param reader         For reading stream events. This class is responsible for closing the reader.
   */
  public LevelDBStreamFileConsumer(CConfiguration cConf,
                                   StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                   FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                   StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
                                   @Nullable ReadFilter extraFilter,
                                   LevelDBOcTableCore tableCore, Object dbLock) {
    super(cConf, streamConfig, consumerConfig, reader, stateStore, beginConsumerState, extraFilter);
    this.tableCore = tableCore;
    this.dbLock = dbLock;
    this.rowMapForClaim = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    this.colMapForClaim = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  }

  @Override
  protected boolean claimFifoEntry(byte[] row, byte[] value, byte[] oldValue) throws IOException {
    synchronized (dbLock) {
      Map<byte[], byte[]> values =
        tableCore.getRow(row, new byte[][] { stateColumnName }, null, null, -1, Transaction.ALL_VISIBLE_LATEST);
      if (!Arrays.equals(values.get(stateColumnName), oldValue)) {
        return false;
      }

      rowMapForClaim.clear();
      colMapForClaim.clear();
      colMapForClaim.put(stateColumnName, value);
      rowMapForClaim.put(row, colMapForClaim);

      tableCore.persist(rowMapForClaim, KeyValue.LATEST_TIMESTAMP);
      return true;
    }
  }

  @Override
  protected void updateState(Iterable<byte[]> rows, int size, byte[] value) throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] row : rows) {
      NavigableMap<byte[], byte[]> values = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      values.put(stateColumnName, value);
      changes.put(row, values);
    }
    tableCore.persist(changes, KeyValue.LATEST_TIMESTAMP);
  }

  @Override
  protected void undoState(Iterable<byte[]> rows, int size) throws IOException {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> changes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (byte[] row : rows) {
      NavigableMap<byte[], byte[]> values = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      values.put(stateColumnName, DUMMY_STATE_CONTENT);
      changes.put(row, values);
    }
    tableCore.undo(changes, KeyValue.LATEST_TIMESTAMP);
  }

  @Override
  protected StateScanner scanStates(byte[] startRow, byte[] stopRow) throws IOException {
    final Scanner scanner = tableCore.scan(startRow, stopRow, null, null, Transaction.ALL_VISIBLE_LATEST);
    return new StateScanner() {

      private ImmutablePair<byte[], Map<byte[], byte[]>> pair;

      @Override
      public boolean nextStateRow() throws IOException {
        pair = scanner.next();
        return pair != null;
      }

      @Override
      public byte[] getRow() {
        return pair.getFirst();
      }

      @Override
      public byte[] getState() {
        return pair.getSecond().get(stateColumnName);
      }

      @Override
      public void close() throws IOException {
        scanner.close();
      }
    };
  }
}
