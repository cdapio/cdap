/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data.stream.StreamEventOffset;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.stream.AbstractStreamFileConsumer;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumerState;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.continuuity.hbase.wd.AbstractRowKeyDistributor;
import com.continuuity.hbase.wd.DistributedScanner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamConsumer} that uses HTable to store consuming states.
 */
@NotThreadSafe
public final class HBaseStreamFileConsumer extends AbstractStreamFileConsumer {

  private final HTable hTable;
  private final AbstractRowKeyDistributor keyDistributor;
  private final ExecutorService scanExecutor;

  /**
   * Constructor.
   *
   * @param streamConfig Stream configuration.
   * @param consumerConfig Consumer configuration.
   * @param hTable For communicate with HBase for storing polled entry states (not consumer state). This class is
   *               responsible for closing the HTable.
   * @param reader For reading stream events. This class is responsible for closing the reader.
   */
  public HBaseStreamFileConsumer(CConfiguration cConf,
                                 StreamConfig streamConfig, ConsumerConfig consumerConfig, HTable hTable,
                                 FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                 StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
                                 @Nullable ReadFilter extraFilter,
                                 AbstractRowKeyDistributor keyDistributor) {
    super(cConf, streamConfig, consumerConfig, reader, stateStore, beginConsumerState, extraFilter);
    this.hTable = hTable;
    this.keyDistributor = keyDistributor;
    this.scanExecutor = createScanExecutor(streamConfig.getName());
  }

  @Override
  protected void doClose() throws IOException {
    scanExecutor.shutdownNow();
    hTable.close();
  }

  @Override
  protected boolean claimFifoEntry(byte[] row, byte[] value, byte[] oldValue) throws IOException {
    Put put = new Put(keyDistributor.getDistributedKey(row));
    put.add(QueueEntryRow.COLUMN_FAMILY, stateColumnName, value);
    return hTable.checkAndPut(put.getRow(), QueueEntryRow.COLUMN_FAMILY, stateColumnName, oldValue, put);
  }

  @Override
  protected void updateState(Iterable<byte[]> rows, int size, byte[] value) throws IOException {
    List<Put> puts = Lists.newArrayListWithCapacity(size);

    for (byte[] row : rows) {
      Put put = new Put(keyDistributor.getDistributedKey(row));
      put.add(QueueEntryRow.COLUMN_FAMILY, stateColumnName, value);
      puts.add(put);
    }
    hTable.put(puts);
    hTable.flushCommits();
  }

  @Override
  protected void undoState(Iterable<byte[]> rows, int size) throws IOException {
    List<Delete> deletes = Lists.newArrayListWithCapacity(size);
    for (byte[] row : rows) {
      Delete delete = new Delete(keyDistributor.getDistributedKey(row));
      delete.deleteColumns(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
      deletes.add(delete);
    }
    hTable.delete(deletes);
    hTable.flushCommits();
  }

  @Override
  protected StateScanner scanStates(byte[] startRow, byte[] stopRow) throws IOException {
    Scan scan = new Scan(startRow, stopRow);
    scan.setMaxVersions(1);
    scan.addColumn(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
    scan.setCaching(MAX_SCAN_ROWS);
    // TODO: Add filter for getting committed processed rows only. Need to refactor HBaseQueue2Consumer to extract that.
    final ResultScanner scanner = DistributedScanner.create(hTable, scan, keyDistributor, scanExecutor);
    return new StateScanner() {

      private Result result;

      @Override
      public boolean nextStateRow() throws IOException {
        result = scanner.next();
        return result != null;
      }

      @Override
      public byte[] getRow() {
        return keyDistributor.getOriginalKey(result.getRow());
      }

      @Override
      public byte[] getState() {
        return result.value();
      }

      @Override
      public void close() throws IOException {
        scanner.close();
      }
    };
  }

  private ExecutorService createScanExecutor(String name) {
    ThreadFactory threadFactory = Threads.newDaemonThreadFactory(String.format("stream-%s-consumer-scanner-", name));
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 20, 60, TimeUnit.SECONDS,
                                                         new SynchronousQueue<Runnable>(), threadFactory);
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }
}
