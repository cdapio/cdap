import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.inmemory.DetachedTxSystemClient;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SecondaryIndexTable implements HTableInterface {
  private byte[] secondaryIndex;
  private byte[] secondaryIndexFamily;
  private TransactionAwareHTable transactionAwareHTable;
  private TransactionAwareHTable secondaryIndexTable;
  private TransactionContext transactionContext;

  public SecondaryIndexTable(HTable hTable, HTable secondaryTable, byte[] secondaryIndex) {
    this.secondaryIndex = secondaryIndex;
    this.secondaryIndexFamily = Bytes.toBytes("indexFamily");
    this.transactionAwareHTable = new TransactionAwareHTable(hTable);
    this.secondaryIndexTable = new TransactionAwareHTable(secondaryTable);
    this.transactionContext = new TransactionContext(new DetachedTxSystemClient(), transactionAwareHTable,
                                                secondaryIndexTable);
  }

  @Override
  public byte[] getTableName() {
    return transactionAwareHTable.getTableName();
  }

  @Override
  public TableName getName() {
    return transactionAwareHTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return transactionAwareHTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {

    return transactionAwareHTable.getTableDescriptor();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    try {
      transactionContext.start();
      boolean exists = transactionAwareHTable.exists(get);
      transactionContext.finish();
      return exists;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction");
      }
    }
    return false;
  }

  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    return transactionAwareHTable.exists(gets);
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {

  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws
    IOException, InterruptedException {

  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public Result get(Get get) throws IOException {
    try {
      transactionContext.start();
      Result result = transactionAwareHTable.get(get);
      transactionContext.finish();
      return result;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction");
      }
    }
    return null;
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    try {
      transactionContext.start();
      Result[] result = transactionAwareHTable.get(gets);
      transactionContext.finish();
      return result;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction");
      }
    }
    return null;
  }

  public Result[] getByIndex(byte[] value) throws IOException {
    try {
      transactionContext.start();
      Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
      ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

      ArrayList<Get> gets = new ArrayList<Get>();
      for (Result result : indexScanner) {
        for (Cell cell : result.listCells()) {
          gets.add(new Get(cell.getValue()));
        }
      }
      Result[] results = transactionAwareHTable.get(gets);
      transactionContext.finish();
      return results;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction");
      }
    }
    return null;
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
    try {
      transactionContext.start();
      Put secondaryIndexPut = new Put(put.getRow());
      Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
      for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
        for (KeyValue value : family.getValue()) {
          if (value.getQualifier().equals(secondaryIndex)) {
            byte[] secondaryQualifier = Bytes.add(value.getQualifier(), value.getValue(), value.getRow());
            secondaryIndexPut.add(secondaryIndexFamily, secondaryQualifier, value.getValue());
          }
        }
      }
      transactionAwareHTable.put(put);
      secondaryIndexTable.put(secondaryIndexPut);

      transactionContext.finish();
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction");
      }
    }
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
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws
    IOException {
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
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
                                   Durability durability) throws IOException {
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
    throws IOException {
    return 0;
  }

  @Override
  public boolean isAutoFlush() {
    return transactionAwareHTable.isAutoFlush();
  }

  @Override
  public void flushCommits() throws IOException {
    transactionAwareHTable.flushCommits();
    secondaryIndexTable.flushCommits();
  }

  @Override
  public void close() throws IOException {
    transactionAwareHTable.close();
    secondaryIndexTable.close();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return transactionAwareHTable.coprocessorService(row);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                                  Batch.Call<T, R> callable) throws ServiceException,
    Throwable {
    return transactionAwareHTable.coprocessorService(service, startKey, endKey, callable);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                        Batch.Call<T, R> callable, Batch.Callback<R> callback) throws
    ServiceException, Throwable {
    transactionAwareHTable.coprocessorService(service, startKey, endKey, callable, callback);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    transactionAwareHTable.setAutoFlush(autoFlush);
    secondaryIndexTable.setAutoFlush(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    transactionAwareHTable.setAutoFlush(autoFlush, clearBufferOnFail);
    secondaryIndexTable.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    transactionAwareHTable.setAutoFlushTo(autoFlush);
    secondaryIndexTable.setAutoFlushTo(autoFlush);
  }

  @Override
  public long getWriteBufferSize() {
    return transactionAwareHTable.getWriteBufferSize();
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    transactionAwareHTable.setWriteBufferSize(writeBufferSize);
    secondaryIndexTable.setWriteBufferSize(writeBufferSize);
  }
}
