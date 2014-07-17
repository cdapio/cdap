/*
 * Copyright 2014 Continuuity, Inc.
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
package com.continuuity.data2.transaction.hbase;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionCodec;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * A Transaction Aware HTable implementation for HBase 0.96. Operations are committed as usual,
 * but upon a failed or aborted transaction, they are rolled back to the state before the transaction
 * was started.
 */
public class TransactionAwareHTable implements HTableInterface, TransactionAware {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionAwareHTable.class);
  private Transaction tx;
  private final HTable hTable;
  private final TransactionCodec txCodec;
  private final List<ActionChange> changeSet;
  private boolean allowNonTransactional;

  /**
   * Create a transactional aware instance of the passed HTable
   * @param hTable
   */
  public TransactionAwareHTable(HTable hTable) {
    this(hTable, false);
  }

  /**
   * Create a transactional aware instance of the passed HTable, with the option
   * of allowing non-transactional operations.
   * @param hTable
   * @param allowNonTransactional
   */
  public TransactionAwareHTable(HTable hTable, boolean allowNonTransactional) {
    this.hTable = hTable;
    this.changeSet = new ArrayList<ActionChange>();
    this.txCodec = new TransactionCodec();
    this.allowNonTransactional = allowNonTransactional;
  }

  /**
   * True if the instance allows non-transaction operations.
   * @return
   */
  public boolean getAllowNonTransactional() {
    return this.allowNonTransactional;
  }

  /**
   * Set whether the instance allows non-transactional operations.
   * @param allowNonTransactional
   */
  public void setAllowNonTransactional(boolean allowNonTransactional) {
    this.allowNonTransactional = allowNonTransactional;
  }

  @Override
  public byte[] getTableName() {
    return hTable.getTableName();
  }

  @Override
  public TableName getName() {
    return hTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return hTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return hTable.getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.exists(transactionalizeAction(get));
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    List<Get> transactionalizedGets = new ArrayList<Get>(gets.size());
    for (Get get : gets) {
      transactionalizedGets.add(transactionalizeAction(get));
    }
    return hTable.exists(transactionalizedGets);
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    hTable.batch(transactionalizeActions(actions), results);
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.batch(transactionalizeActions(actions));
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws
    IOException, InterruptedException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    hTable.batchCallback(transactionalizeActions(actions), results, callback);
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException,
    InterruptedException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.batchCallback(transactionalizeActions(actions), callback);
  }

  @Override
  public Result get(Get get) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.get(transactionalizeAction(get));
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    ArrayList<Get> transactionalizedGets = new ArrayList<Get>();
    for (Get get : gets) {
      transactionalizedGets.add(transactionalizeAction(get));
    }
    return hTable.get(transactionalizedGets);
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    if (allowNonTransactional) {
      return hTable.getRowOrBefore(row, family);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    return hTable.getScanner(transactionalizeAction(scan));
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    Scan scan = new Scan();
    scan.addFamily(family);
    return hTable.getScanner(transactionalizeAction(scan));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return hTable.getScanner(transactionalizeAction(scan));
  }

  @Override
  public void put(Put put) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    Put txPut = transactionalizeAction(put);
    hTable.put(txPut);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    List<Put> transactionalizedPuts = new ArrayList<Put>(puts.size());
    for (Put put : puts) {
      Put txPut = transactionalizeAction(put);
      transactionalizedPuts.add(txPut);
    }
    hTable.put(transactionalizedPuts);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    if (allowNonTransactional) {
      return hTable.checkAndPut(row, family, qualifier, value, put);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public void delete(Delete delete) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    put(transactionalizeAction(delete));
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    List<Put> transactionalizedPuts = new ArrayList<Put>(deletes.size());
    for (Delete delete : deletes) {
      Put txPut = transactionalizeAction(delete);
      transactionalizedPuts.add(txPut);
    }
    hTable.put(transactionalizedPuts);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
    throws IOException {
    if (allowNonTransactional) {
      return hTable.checkAndDelete(row, family, qualifier, value, delete);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    if (tx == null) {
      throw new IOException("Transaction not started");
    }
    RowMutations transactionalMutations = new RowMutations();
    for (Mutation mutation : rm.getMutations()) {
      if (mutation instanceof Put) {
        transactionalMutations.add(transactionalizeAction((Put) mutation));
      } else if (mutation instanceof Delete) {
        transactionalMutations.add(transactionalizeAction((Delete) mutation));
      }
    }
    hTable.mutateRow(transactionalMutations);
  }

  @Override
  public Result append(Append append) throws IOException {
    if (allowNonTransactional) {
      return hTable.append(append);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    if (allowNonTransactional) {
      return hTable.increment(increment);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    if (allowNonTransactional) {
      return hTable.incrementColumnValue(row, family, qualifier, amount);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
    throws IOException {
    if (allowNonTransactional) {
      return hTable.incrementColumnValue(row, family, qualifier, amount, durability);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
    throws IOException {
    if (allowNonTransactional) {
      return hTable.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    } else {
      throw new UnsupportedOperationException("Operation is not supported transactionally");
    }
  }

  @Override
  public boolean isAutoFlush() {
    return hTable.isAutoFlush();
  }

  @Override
  public void flushCommits() throws IOException {
    hTable.flushCommits();
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return hTable.coprocessorService(row);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                                  Batch.Call<T, R> callable)
    throws ServiceException, Throwable {
    return hTable.coprocessorService(service, startKey, endKey, callable);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                        Batch.Call<T, R> callable, Batch.Callback<R> callback)
    throws ServiceException, Throwable {
    hTable.coprocessorService(service, startKey, endKey, callable, callback);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlushTo(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    hTable.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    hTable.setAutoFlushTo(autoFlush);
  }

  @Override
  public long getWriteBufferSize() {
    return hTable.getWriteBufferSize();
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    hTable.setWriteBufferSize(writeBufferSize);
  }

  @Override
  public void startTx(Transaction tx) {
    this.tx = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    List<byte[]> txChanges = new ArrayList<byte[]>();
    for (ActionChange change : changeSet) {
      txChanges.add(Bytes.add(getTableName(), change.getRow(),
                              Bytes.add(change.getFamily(), change.getQualifier())));
    }
    return txChanges;
  }

  @Override
  public boolean commitTx() throws Exception {
    hTable.flushCommits();
    return true;
  }

  @Override
  public void postTxCommit() {
    tx = null;
    changeSet.clear();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    try {
      List<Delete> rollbackDeletes = new ArrayList<Delete>(changeSet.size());
      for (ActionChange change : changeSet) {
        byte[] row = change.getRow();
        byte[] family = change.getFamily();
        byte[] qualifier = change.getQualifier();
        long transactionTimestamp = tx.getWritePointer();
        Delete rollbackDelete = new Delete(row, transactionTimestamp);
        if (family != null && qualifier == null) {
          rollbackDelete.deleteFamily(family, transactionTimestamp);
        } else if (family != null && qualifier != null) {
          rollbackDelete.deleteColumn(family, qualifier, transactionTimestamp);
        }
        rollbackDeletes.add(rollbackDelete);
      }
      hTable.delete(rollbackDeletes);
      return true;
    } finally {
      try {
        hTable.flushCommits();
      } catch (Exception e) {
        LOG.error("Could not flush HTable commits", e);
      }
      tx = null;
      changeSet.clear();
    }
  }

  @Override
  public String getTransactionAwareName() {
    return Bytes.toString(getTableName());
  }

  /**
   * Record of each transaction that causes a change. This reference is used to rollback
   * any operation upon failure.
   */
  private class ActionChange {
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;

    private ActionChange(byte[] row, byte[] family, byte[] qualifier) {
      this.row = row;
      this.family = family;
      this.qualifier = qualifier;
    }

    private byte[] getRow() {
      return row;
    }

    private byte[] getFamily() {
      return family;
    }

    private byte[] getQualifier() {
      return qualifier;
    }
  }

  // Helpers to get copies of objects with the timestamp set to the current transaction timestamp.

  private Get transactionalizeAction(Get get) throws IOException {
    txCodec.addToOperation(get, tx);
    return get;
  }

  private Scan transactionalizeAction(Scan scan) throws IOException {
    txCodec.addToOperation(scan, tx);
    return scan;
  }

  private Put transactionalizeAction(Put put) throws IOException {
    Put txPut = new Put(put.getRow(), tx.getWritePointer());
    Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
    if (!familyMap.isEmpty()) {
      for (Map.Entry<byte[], List<KeyValue>> family : familyMap) {
        List<KeyValue> familyValues = family.getValue();
        if (!familyValues.isEmpty()) {
          for (KeyValue value : familyValues) {
            txPut.add(value.getFamily(), value.getQualifier(), tx.getWritePointer(), value.getValue());
            changeSet.add(new ActionChange(txPut.getRow(), value.getFamily(), value.getQualifier()));
          }
        }
      }
    }
    for (Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet()) {
      txPut.setAttribute(entry.getKey(), entry.getValue());
    }
    txPut.setWriteToWAL(put.getWriteToWAL());
    txCodec.addToOperation(txPut, tx);
    return txPut;
  }

  private Put transactionalizeAction(Delete delete) throws IOException {
    long transactionTimestamp = tx.getWritePointer();

    byte[] deleteRow = delete.getRow();
    Put txPut = new Put(deleteRow, transactionTimestamp);

    Map<byte[], List<KeyValue>> familyToDelete = delete.getFamilyMap();
    if (familyToDelete.isEmpty()) {
      Result result = get(new Get(delete.getRow()));
      // Delete everything
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = result.getNoVersionMap();
      for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry : resultMap.entrySet()) {
        NavigableMap<byte[], byte[]> familyColumns = result.getFamilyMap(familyEntry.getKey());
        for (Map.Entry<byte[], byte[]> column : familyColumns.entrySet()) {
          txPut.add(familyEntry.getKey(), column.getKey(), transactionTimestamp, new byte[0]);
          changeSet.add(new ActionChange(deleteRow, familyEntry.getKey(), column.getKey()));
        }
      }
    } else {
      for (Map.Entry<byte [], List<KeyValue>> familyEntry : familyToDelete.entrySet()) {
        byte[] family = familyEntry.getKey();
        List<KeyValue> entries = familyEntry.getValue();
        if (entries.isEmpty()) {
          Result result = get(new Get(delete.getRow()));
          // Delete entire family
          NavigableMap<byte[], byte[]> familyColumns = result.getFamilyMap(family);
          for (Map.Entry<byte[], byte[]> column : familyColumns.entrySet()) {
            txPut.add(family, column.getKey(), transactionTimestamp, new byte[0]);
            changeSet.add(new ActionChange(deleteRow, family, column.getKey()));
          }
        } else {
          for (KeyValue value : entries) {
            txPut.add(value.getFamily(), value.getQualifier(), transactionTimestamp, new byte[0]);
            changeSet.add(new ActionChange(deleteRow, value.getFamily(), value.getQualifier()));
          }
        }
      }
    }
    return txPut;
  }

  private List<? extends Row> transactionalizeActions(List<? extends Row> actions) throws IOException {
    List<Row> transactionalizedActions = new ArrayList<Row>(actions.size());
    for (Row action : actions) {
      if (action instanceof Get) {
        transactionalizedActions.add(transactionalizeAction((Get) action));
      } else if (action instanceof Put) {
        transactionalizedActions.add(transactionalizeAction((Put) action));
      } else if (action instanceof Delete) {
        transactionalizedActions.add(transactionalizeAction((Delete) action));
      } else {
        transactionalizedActions.add(action);
      }
    }
    return transactionalizedActions;
  }
}
