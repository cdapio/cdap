/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 * Constants for queue implementation in HBase.
 */
public final class QueueUtils {

  public static String determineQueueConfigTableName(String queueTableName) {
    // the name of this table has the form: <reactor name space>.<system name space>.(queue|stream).*
    // beware that the reactor name space may also contain ., but there must be at least two .

    int firstDot = queueTableName.indexOf('.');
    if (firstDot < 0) {
      throw new IllegalArgumentException(
        "Unable to determine config table name from queue table name '" + queueTableName + "'");
    }
    int secondDot = queueTableName.indexOf('.', firstDot + 1);
    if (secondDot < 0) {
      throw new IllegalArgumentException(
        "Unable to determine config table name from queue table name '" + queueTableName + "'");
    }
    int qpos = queueTableName.indexOf(QueueConstants.QUEUE_TABLE_PREFIX, secondDot + 1);
    int spos = queueTableName.indexOf(QueueConstants.STREAM_TABLE_PREFIX, secondDot + 1);
    int pos;
    if (qpos < 0) {
      pos = spos;
    } else if (spos < 0) {
      pos = qpos;
    } else {
      pos = Math.min(qpos, spos);
    }
    if (pos < 0) {
      throw new IllegalArgumentException(
        "Unable to determine config table name from queue table name '" + queueTableName + "'");
    }
    return queueTableName.substring(0, pos) + QueueConstants.QUEUE_CONFIG_TABLE_NAME;
  }

  private QueueUtils() { }
}
