/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.save;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.ImmutableList;

import java.util.concurrent.Callable;

/**
 * Manages reading/writing of checkpoint information for a topic and partition.
 */
public final class CheckpointManager {

  private static final byte [] ROW_KEY_PREFIX = Bytes.toBytes(100);
  private static final byte [] OFFSET_COLNAME = Bytes.toBytes("nextOffset");

  private final TransactionExecutor txExecutor;
  private final OrderedColumnarTable metaTable;
  private final byte [] rowKeyPrefix;

  public CheckpointManager(OrderedColumnarTable metaTable,
                           TransactionSystemClient txClient,
                           String topic) {
    this.metaTable = metaTable;
    this.txExecutor = new DefaultTransactionExecutor(txClient, ImmutableList.of((TransactionAware) metaTable));
    this.rowKeyPrefix = Bytes.add(ROW_KEY_PREFIX, Bytes.toBytes(topic));
  }

  public void saveCheckpoint(final int partition, final long nextOffset) throws Exception {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        metaTable.put(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)), OFFSET_COLNAME, Bytes.toBytes(nextOffset));
      }
    });
  }

  public long getCheckpoint(final int partition) throws Exception {
    Long checkpoint = txExecutor.execute(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        byte [] result =
          metaTable.get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)), OFFSET_COLNAME);
        if (result == null) {
          return null;
        }

        return Bytes.toLong(result);
      }
    });

    return checkpoint == null ? -1 : checkpoint;
  }
}
