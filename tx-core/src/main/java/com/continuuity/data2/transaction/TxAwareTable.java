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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TxAwareTable implements HTableInterface, TransactionAware {
  private Transaction tx;
  private HTable hTable;
  private HashMap<byte[], Row> currentOperations;

  public TxAwareTable(HTable hTable) {
    this.hTable = hTable;
  }

  @Override
  public byte[] getTableName() {
    return this.hTable.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return this.hTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return null;
  }

  @Override
  public boolean exists(Get get) throws IOException {
    return false;
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {

  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public Result get(Get get) throws IOException {
    try {
      currentOperations.put(get.getRow(), get);
      return hTable.get(get);
    } catch (IOException e) {
      try {
        rollbackTx();
      } catch (Exception e1) {
        Throwables.propagate(new IOException("Could not rollback get on failure"));
      }
    }
    return null;
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    Result[] results = new Result[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      results[i] = get(gets.get(i));
    }
    return results;
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    return null;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return null;
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return null;
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return null;
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
    return false;
  }

  @Override
  public void flushCommits() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    return null;
  }

  @Override
  public void unlockRow(RowLock rl) throws IOException {

  }

  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
    return null;
  }

  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol, byte[] startKey,
                                                                           byte[] endKey, Batch.Call<T,
    R> callable) throws IOException, Throwable {
    return null;
  }

  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey,
                                                                 Batch.Call<T, R> callable, Batch.Callback<R>
    callback) throws IOException, Throwable {

  }

  @Override
  public void setAutoFlush(boolean autoFlush) {

  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {

  }

  @Override
  public long getWriteBufferSize() {
    return 0;
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {

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
    currentOperations.clear();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    for (Map.Entry<byte[], Row> entry : currentOperations.entrySet()) {
      byte[] rowId = entry.getKey();
      Row operation = entry.getValue();
      if (operation instanceof Get) {
        // Do nothing on rollback for reads;
      } else if (operation instanceof Put) {
        // TODO : delete operations on each put that failed
      }
    }
    return true;
  }

  @Override
  public String getName() {
    return Bytes.toString(getTableName());
  }
}
