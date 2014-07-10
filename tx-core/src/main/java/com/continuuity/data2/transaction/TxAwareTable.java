package com.continuuity.data2.transaction;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TxAwareTable implements HTableInterface, TransactionAware {
  private Transaction tx;
  private HTable hTable;
  private ArrayList<Row> failedTransactions;

  public TxAwareTable(HTable hTable) {
    this.hTable = hTable;
    this.failedTransactions = new ArrayList<Row>();
  }

  @Override
  public byte[] getTableName() {
    return hTable.getTableName();
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
    return hTable.exists(get);
  }

  // TODO: Rollback only failed transactions or all?
  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
    try {
      hTable.batch(actions, results);
    } catch (Exception e) {
      try {
        for (int i = 0; i < results.length; i++) {
          if (results[i] == null) {
            failedTransactions.add(actions.get(i));
          }
        }
        rollbackTx();
      } catch (Exception e1) {
        Throwables.propagate(new Exception("Could not rollback failed transactions"));
      }
    }
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public Result get(Get get) throws IOException {
    return hTable.get(get);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return hTable.get(gets);
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    return hTable.getRowOrBefore(row, family);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return hTable.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return hTable.getScanner(family);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return hTable.getScanner(family, qualifier);
  }

  @Override
  public void put(Put put) throws IOException {

  }

  @Override
  public void put(List<Put> puts) throws IOException {

  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
    return false;
  }

  @Override
  public void delete(Delete delete) throws IOException {

  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {

  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    return false;
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {

  }

  @Override
  public Result append(Append append) throws IOException {
    return null;
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return null;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
    throws IOException {
    return 0;
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

  // TODO: This seems to have been deprecated. Should we just not support it, then?
  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    return hTable.lockRow(row);
  }

  @Override
  public void unlockRow(RowLock rl) throws IOException {
    hTable.unlockRow(rl);
  }

  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
    return hTable.coprocessorProxy(protocol, row);
  }

  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol, byte[] startKey,
                                                                           byte[] endKey, Batch.Call<T,
    R> callable) throws IOException, Throwable {
    return hTable.coprocessorExec(protocol, startKey, endKey, callable);
  }

  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey,
                                                                 Batch.Call<T, R> callable, Batch.Callback<R>
    callback) throws IOException, Throwable {
    hTable.coprocessorExec(protocol, startKey, endKey, callable, callback);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    hTable.setAutoFlush(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    hTable.setAutoFlush(autoFlush, clearBufferOnFail);
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
    return null;
  }

  @Override
  public boolean commitTx() throws Exception {
    return false;
  }

  @Override
  public void postTxCommit() {
    this.tx = null;
    this.failedTransactions.clear();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return false;
  }

  @Override
  public String getName() {
    return Bytes.toString(getTableName());
  }
}
