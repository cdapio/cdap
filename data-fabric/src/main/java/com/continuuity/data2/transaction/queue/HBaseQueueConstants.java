/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 *
 */
public final class HBaseQueueConstants {

  public static final byte[] COLUMN_FAMILY = new byte[] {'q'};
  public static final byte[] DATA_COLUMN = new byte[] {'d'};
  public static final byte[] META_COLUMN = new byte[] {'m'};
  public static final byte[] STATE_COLUMN_PREFIX = new byte[] {'s'};

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait for 5 seconds for table creation.


  private HBaseQueueConstants() {
  }
}
