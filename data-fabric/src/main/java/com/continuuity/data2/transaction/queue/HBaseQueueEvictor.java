/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link QueueEvictor} for HBase queue.
 */
public final class HBaseQueueEvictor implements QueueEvictor {

  private final HTable hTable;
  private final Executor executor;
  private final byte[] startRow;
  private final int numGroups;

  public HBaseQueueEvictor(HTable hTable, QueueName queueName, Executor executor, int numGroups) {
    this.hTable = hTable;
    this.executor = executor;
    this.startRow = QueueUtils.getQueueRowPrefix(queueName);
    this.numGroups = numGroups;
  }

  @Override
  public ListenableFuture<Integer> evict(final Transaction transaction) {
    final byte[] endRow = getEndRow(transaction);
    final SettableFuture<Integer> result = SettableFuture.create();
    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          final AtomicInteger count = new AtomicInteger();
          hTable.coprocessorExec(HBaseQueueEvictionProtocol.class, startRow, endRow,
                                 new Batch.Call<HBaseQueueEvictionProtocol, Integer>() {

            @Override
            public Integer call(HBaseQueueEvictionProtocol protocol) throws IOException {
              Scan scan = new Scan();
              scan.setStartRow(startRow);
              scan.setStopRow(endRow);

              return protocol.evict(scan, transaction.getReadPointer(), transaction.getExcludedList(), numGroups);
            }
          }, new Batch.Callback<Integer>() {
            @Override
            public void update(byte[] region, byte[] row, Integer result) {
              count.addAndGet(result);
            }
          });

          result.set(count.get());
        } catch (Throwable t) {
          result.setException(t);
        }
      }
    });
    return result;
  }

  private byte[] getEndRow(Transaction transaction) {
    return Bytes.add(startRow, Bytes.toBytes(transaction.getReadPointer() + 1));
  }
}
