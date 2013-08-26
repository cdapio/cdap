/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.QueueEvictor;
import com.continuuity.data2.transaction.queue.QueueUtils;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link com.continuuity.data2.transaction.queue.QueueEvictor} for HBase queue.
 */
public final class HBaseQueueEvictor implements QueueEvictor {

  private final HTable hTable;
  private final Executor executor;
  private final byte[] rowPrefix;
  private final int numGroups;

  public HBaseQueueEvictor(HTable hTable, QueueName queueName, Executor executor, int numGroups) {
    this.hTable = hTable;
    this.executor = executor;
    this.rowPrefix = QueueUtils.getQueueRowPrefix(queueName);
    this.numGroups = numGroups;
  }

  @Override
  public ListenableFuture<Integer> evict(final Transaction transaction) {
    final byte[] endRow = QueueUtils.getStopRowForTransaction(rowPrefix, transaction);
    final SettableFuture<Integer> result = SettableFuture.create();
    executor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          final AtomicInteger count = new AtomicInteger();
          hTable.coprocessorExec(HBaseQueueEvictionProtocol.class, rowPrefix, endRow,
                                 new Batch.Call<HBaseQueueEvictionProtocol, Integer>() {

            @Override
            public Integer call(HBaseQueueEvictionProtocol protocol) throws IOException {
              Scan scan = new Scan();
              scan.setStartRow(rowPrefix);
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
}
