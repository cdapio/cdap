/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.hbase;

import com.continuuity.data.file.FileReader;
import com.continuuity.data.stream.StreamEventOffset;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.transaction.queue.QueueEntryRow;
import com.continuuity.data2.transaction.stream.AbstractStreamFileConsumer;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamConsumer} that uses HTable to store consuming states.
 */
// TODO: Pre-split table
@NotThreadSafe
public final class HBaseStreamFileConsumer extends AbstractStreamFileConsumer {

  private final HTable hTable;

  /**
   *
   * @param streamConfig
   * @param consumerConfig
   * @param hTable For communicate with HBase for storing polled entry states (not consumer state). This class is
   *               responsible for closing the HTable.
   * @param reader For reading stream events. This class is responsible for closing the reader.
   */
  public HBaseStreamFileConsumer(StreamConfig streamConfig, ConsumerConfig consumerConfig, HTable hTable,
                                 FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                 HBaseStreamConsumerStateStore stateStore) {
    super(streamConfig, consumerConfig, reader, stateStore);
    this.hTable = hTable;
  }

  @Override
  protected void doClose() throws IOException {
    hTable.close();
  }

  @Override
  protected boolean claimFifoEntry(byte[] row, byte[] value) throws IOException {
    Put put = new Put(row);
    put.add(QueueEntryRow.COLUMN_FAMILY, stateColumnName, value);
    return hTable.checkAndPut(put.getRow(), QueueEntryRow.COLUMN_FAMILY, stateColumnName, null, put);
  }

  @Override
  protected void updateState(Iterable<byte[]> rows, int size, byte[] value) throws IOException {
    List<Put> puts = Lists.newArrayListWithCapacity(size);

    for (byte[] row : rows) {
      Put put = new Put(row);
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
      Delete delete = new Delete(row);
      delete.deleteColumn(QueueEntryRow.COLUMN_FAMILY, stateColumnName);
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
    // TODO: Add filter for getting committed processed rows only. Need to refactor HBaseQueue2Consumer to extra that.
    final ResultScanner scanner = hTable.getScanner(scan);
    return new StateScanner() {

      private Result result;

      @Override
      public boolean nextStateRow() throws IOException {
        result = scanner.next();
        return result != null;
      }

      @Override
      public byte[] getRow() {
        return result.getRow();
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

}
