/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 * Representing a queue entry fetched from persisted storage.
 */
public final class SimpleQueueEntry {
  private final byte[] rowKey;
  private final byte[] data;
  private final byte[] state;

  SimpleQueueEntry(byte[] rowKey, byte[] data, byte[] state) {
    this.rowKey = rowKey;
    this.data = data;
    this.state = state;
  }

  public byte[] getRowKey() {
    return rowKey;
  }

  public byte[] getData() {
    return data;
  }

  public byte[] getState() {
    return state;
  }
}
