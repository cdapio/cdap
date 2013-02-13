package com.continuuity.data.operation.ttqueue.internal;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Meta data about a queue entry per consumer for FIFO queues
 */
public class EntryConsumerMeta {
  private final EntryState state;
  private final int tries;

  public EntryConsumerMeta(EntryState state, int tries) {
    this.state = state;
    this.tries = tries;
  }

  public boolean isClaimed() {
    return state == EntryState.CLAIMED;
  }

  public boolean isAcked() {
    return state == EntryState.ACKED;
  }

  public EntryState getState() {
    return state;
  }

  public int getTries() {
    return tries;
  }

  public byte [] getBytes() {
    return Bytes.add(this.state.getBytes(), Bytes.toBytes(tries));
  }

  public static EntryConsumerMeta fromBytes(byte [] bytes) {
    return new EntryConsumerMeta(EntryState.fromBytes(new byte[] {bytes[0]}), Bytes.toInt(bytes, 1));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("state", this.state)
      .add("tries", this.tries)
      .toString();
  }

  public static enum EntryState {
    CLAIMED, ACKED;

    private static final byte [] CLAIMED_BYTES = { 0 };
    private static final byte [] ACKED_BYTES = { 1 };

    public byte [] getBytes() {
      switch (this) {
        case CLAIMED: return CLAIMED_BYTES;
        case ACKED:   return ACKED_BYTES;
      }
      return null;
    }

    public static EntryState fromBytes(byte [] bytes) {
      if (bytes.length == 1) {
        if (bytes[0] == CLAIMED_BYTES[0]) return CLAIMED;
        if (bytes[0] == ACKED_BYTES[0]) return ACKED;
      }
      throw new RuntimeException("Invalid deserialization of EntryState");
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("state", this.name())
        .toString();
    }
  }
}
