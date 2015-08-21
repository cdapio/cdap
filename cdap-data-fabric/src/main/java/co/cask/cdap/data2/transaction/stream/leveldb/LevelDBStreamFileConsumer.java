/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.data2.transaction.stream.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.data.stream.StreamEventOffset;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.KeyValue;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.transaction.stream.AbstractStreamFileConsumer;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerState;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.tephra.Transaction;
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

  private final LevelDBTableCore tableCore;
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
                                   LevelDBTableCore tableCore, Object dbLock) {
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

      private Row pair;

      @Override
      public boolean nextStateRow() throws IOException {
        pair = scanner.next();
        return pair != null;
      }

      @Override
      public byte[] getRow() {
        return pair.getRow();
      }

      @Override
      public byte[] getState() {
        return pair.getColumns().get(stateColumnName);
      }

      @Override
      public void close() throws IOException {
        scanner.close();
      }
    };
  }
}
