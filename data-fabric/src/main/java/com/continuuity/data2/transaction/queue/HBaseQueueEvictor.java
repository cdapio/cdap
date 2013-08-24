/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 *
 */
public final class HBaseQueueEvictor implements QueueEvictor {

  private final HTable hTable;
  private final Executor executor;
  private final byte[] startRow;

  public HBaseQueueEvictor(HTable hTable, QueueName queueName, Executor executor) {
    this.hTable = hTable;
    this.executor = executor;
    this.startRow = QueueUtils.getQueueRowPrefix(queueName);
  }

  @Override
  public ListenableFuture<Integer> evict(final Transaction transaction) {
    final byte[] endRow = getEndRow(transaction);

    executor.execute(new Runnable() {

      @Override
      public void run() {
        hTable.coprocessorExec(HBaseQueueEvictionProtocol.class, startRow, endRow, new Batch.Call<HBaseQueueEvictionProtocol, Integer>() {

          @Override
          public Integer call(HBaseQueueEvictionProtocol protocol) throws IOException {
            Scan scan = new Scan();
            scan.setStartRow(startRow);
            scan.setStopRow(endRow);
            
            return protocol.evict(scan, transaction.getReadPointer(), )
          }
        })
      }
    });
//
//    Map<byte[],Integer> results = table.coprocessorExec(HBaseQueueEvictionProtocol.class, null, null,
//                                                        new Batch.Call<HBaseQueueEvictionProtocol, Integer>() {
//                                                          @Override
//                                                          public Integer call(HBaseQueueEvictionProtocol instance) throws IOException {
//                                                            long smallestExclude = Long.MAX_VALUE;
//                                                            if (transaction.getExcludedList().length > 0) {
//                                                              smallestExclude = transaction.getExcludedList()[0];
//                                                            }
//
//                                                            Scan scan = new Scan();
//                                                            scan.setStartRow(QueueUtils.getQueueRowPrefix(queueName));
//                                                            scan.setStopRow(Bytes.add(QueueUtils.getQueueRowPrefix(queueName),
//                                                                                      Bytes.toBytes(transaction.getReadPointer())));
//                                                            return instance.evict(scan, transaction.getReadPointer(), smallestExclude, 1);
//                                                          }
//                                                        });
//
  }

  private byte[] getEndRow(Transaction transaction) {
    return Bytes.add(startRow, Bytes.toBytes(transaction.getReadPointer() + 1));
  }
}
