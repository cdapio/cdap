package com.continuuity.performance.data2.transaction.persist;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class TransactionEdit implements Writable {
  public enum State {
    INPROGRESS, COMMITTING, COMMITTED, INVALID;
  }

  private long txId;
  private State state;
  private byte[] payload;

  // For Writable serialization
  public TransactionEdit() {}

  public TransactionEdit(long txId, State state, byte[] payload) {
    this.txId = txId;
    this.state = state;
    this.payload = payload;
  }

  public long getTxId() {
    return txId;
  }

  public State getState() {
    return state;
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(txId);
    // use ordinal for predictable size, though this does not support evolution
    out.writeInt(state.ordinal());
    if (payload == null) {
      out.writeInt(0);
    } else {
      out.writeInt(payload.length);
      out.write(payload);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    txId = in.readLong();
    int stateIdx = in.readInt();
    try {
      state = State.values()[stateIdx];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IOException("State enum ordinal value is out of range: "+stateIdx);
    }
    payload = new byte[in.readInt()];
    in.readFully(payload);
  }

  public byte[] toBytes() {
    int payloadLength = payload != null ? payload.length : 0;
    byte[] bytes = new byte[8 + 4 + 4 + payloadLength];
    int idx = 0;
    System.arraycopy(Bytes.toBytes(txId), 0, bytes, idx, 8);
    idx += 8;
    System.arraycopy(Bytes.toBytes(state.ordinal()), 0 , bytes, idx, 4);
    idx += 4;
    System.arraycopy(Bytes.toBytes(payloadLength), 0, bytes, idx, 4);
    if (payloadLength > 0) {
      System.arraycopy(payload, 0, bytes, idx, payloadLength);
    }

    return bytes;
  }
}