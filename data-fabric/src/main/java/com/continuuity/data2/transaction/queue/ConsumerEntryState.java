/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 * The state of a queue entry with respect to a consumer.
 */
public enum ConsumerEntryState {

  CLAIMED(1),
  PROCESSED(2);

  private final byte state;

  private ConsumerEntryState(int state) {
    this.state = (byte) state;
  }

  public byte getState() {
    return state;
  }

  public static ConsumerEntryState fromState(byte state) {
    switch (state) {
      case 1:
        return CLAIMED;
      case 2:
        return PROCESSED;
      default:
        throw new IllegalArgumentException("Unknown state number " + state);
    }
  }
}
