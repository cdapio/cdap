/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;

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
}
