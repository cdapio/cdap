/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.tephra.Transaction;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 *  Dataset client for snapshot datasets.
 */
public class SnapshotDataset extends AbstractDataset implements Table {

  private static final byte[] METADATA_ROW_KEY = { 'v', 'e', 'r', 's', 'i', 'o', 'n' };
  private static final byte[] METADATA_KEY_COLUMN = { 'v', 'a', 'l', 'u', 'e' };

  private Table metadataTable;
  private Table mainTable;
  private Transaction tx;

  public SnapshotDataset(String name, Table metadataTable, Table mainTable) {
    super(name, metadataTable, mainTable);
    this.metadataTable = metadataTable;
    this.mainTable = mainTable;
  }

  public Long getCurrentVersion() {
    return metadataTable.get(METADATA_ROW_KEY).getLong(METADATA_KEY_COLUMN);
  }

  public long getTransactionId() {
    return tx.getTransactionId();
  }

  public void updateMetaDataTable(long newVersion) {
    Put put = new Put(METADATA_ROW_KEY, METADATA_KEY_COLUMN, newVersion);
    metadataTable.put(put);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public boolean rollbackTx() throws Exception {
    this.tx = null;
    return super.rollbackTx();
  }

  @Override
  public boolean commitTx() throws Exception {
    this.tx = null;
    return super.commitTx();
  }

  @Override
  public Row get(byte[] row) {
    return mainTable.get(row);
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    return mainTable.get(row, column);
  }

  @Override
  public Row get(byte[] row, byte[][] columns) {
    return mainTable.get(row, columns);
  }

  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    return mainTable.get(row, startColumn, stopColumn, limit);
  }

  @Override
  public Row get(Get get) {
    return mainTable.get(get);
  }

  @Override
  public List<Row> get(List<Get> gets) {
    return mainTable.get(gets);
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value) {
    mainTable.put(row, column, value);
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    mainTable.put(row, columns, values);
  }

  @Override
  public void put(Put put) {
    mainTable.put(put);
  }

  @Override
  public void delete(byte[] row) {
    mainTable.delete(row);
  }

  @Override
  public void delete(byte[] row, byte[] column) {
    mainTable.delete(row, column);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    mainTable.delete(row, columns);
  }

  @Override
  public void delete(Delete delete) {
    mainTable.delete(delete);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long amount) {
    return mainTable.incrementAndGet(row, column, amount);
  }

  @Override
  public Row incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
    return mainTable.incrementAndGet(row, columns, amounts);
  }

  @Override
  public Row incrementAndGet(Increment increment) {
    return mainTable.incrementAndGet(increment);
  }

  @Override
  public void increment(byte[] row, byte[] column, long amount) {
    mainTable.increment(row, column, amount);
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    mainTable.increment(row, columns, amounts);
  }

  @Override
  public void increment(Increment increment) {
    mainTable.increment(increment);
  }

  @Override
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow) {
    return mainTable.scan(startRow, stopRow);
  }

  @Override
  public Scanner scan(Scan scan) {
    return mainTable.scan(scan);
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return mainTable.getSplits(numSplits, start, stop);
  }

  @Override
  public boolean compareAndSwap(byte[] key, byte[] keyColumn, byte[] oldValue, byte[] newValue) {
    return mainTable.compareAndSwap(key, keyColumn, oldValue, newValue);
  }

  @Override
  public Type getRecordType() {
    return mainTable.getRecordType();
  }

  @Override
  public List<Split> getSplits() {
    return mainTable.getSplits();
  }

  @Override
  public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
    return mainTable.createSplitRecordScanner(split);
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    return mainTable.createSplitReader(split);
  }

  @Override
  public void write(byte[] bytes, Put put) {
    mainTable.write(bytes, put);
  }

  @Override
  public void close() throws IOException {
    metadataTable.close();
    mainTable.close();
  }
}
