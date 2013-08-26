/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.util.Arrays;

/**
 *
 */
public final class QueueUtils {

  private QueueUtils() {
  }

  public static byte[] getQueueRowPrefix(QueueName queueName) {
    byte[] bytes = Arrays.copyOf(queueName.toBytes(), queueName.toBytes().length);
    int i = 0;
    int j = bytes.length - 1;
    while (i < j) {
      byte tmp = bytes[i];
      bytes[i] = bytes[j];
      bytes[j] = tmp;
      i++;
      j--;
    }
    return bytes;
  }

  /**
   * Gets the stop row for scan up to the read pointer of a transaction. Stop row is queueName + (readPointer + 1).
   */
  public static byte[] getStopRowForTransaction(byte[] queueRowPrefix, Transaction transaction) {
    return Bytes.add(queueRowPrefix, Bytes.toBytes(transaction.getReadPointer() + 1));
  }

  /**
   * Determine whether a column represent the state of a consumer.
   */
  public static boolean isStateColumn(byte[] columnName) {
    return Bytes.startsWith(columnName, QueueConstants.STATE_COLUMN_PREFIX);
  }

  /**
   * For a queue entry consumer state, serialized to byte array, return whether it is processed and committed.
   */
  public static boolean isCommittedProcessed(byte[] stateBytes, Transaction tx) {
    long writePointer = Bytes.toLong(stateBytes, 0, Longs.BYTES);
    if (writePointer > tx.getReadPointer() || Arrays.binarySearch(tx.getExcludedList(), writePointer) >= 0) {
      return false;
    }
    byte state = stateBytes[Longs.BYTES + Ints.BYTES];
    return state == ConsumerEntryState.PROCESSED.getState();
  }

}
