package com.continuuity.data2.transaction.persist;

import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.sun.tools.javac.resources.version;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * Represents a transaction state change in the {@link TransactionLog}.
 */
public class TransactionEdit implements Writable {
  // initial version
  private static final byte VERSION = -1;

  /**
   * The possible state changes for a transaction.
   */
  public enum State {
    INPROGRESS, COMMITTING, COMMITTED, INVALID, ABORTED, MOVE_WATERMARK;
  }

  private long writePointer;
  private long nextWritePointer;
  private long expirationDate;
  private State state;
  private Set<ChangeId> changes = Sets.newHashSet();
  /** Whether or not the COMMITTED change should be fully committed. */
  private boolean canCommit;

  // for Writable
  public TransactionEdit() {
  }

  public TransactionEdit(long writePointer, State state, long expirationDate, Set<ChangeId> changes,
                         long nextWritePointer, boolean canCommit) {
    this.writePointer = writePointer;
    this.state = state;
    this.expirationDate = expirationDate;
    if (changes != null) {
      this.changes = changes;
    }
    this.nextWritePointer = nextWritePointer;
    this.canCommit = canCommit;
  }

  public long getWritePointer() {
    return writePointer;
  }

  public State getState() {
    return state;
  }

  public long getExpiration() {
    return expirationDate;
  }

  public Set<ChangeId> getChanges() {
    return changes;
  }

  public long getNextWritePointer() {
    return nextWritePointer;
  }

  public boolean getCanCommit() {
    return canCommit;
  }

  public static TransactionEdit createStarted(long writePointer, long expirationDate, long nextWritePointer) {
    return new TransactionEdit(writePointer, State.INPROGRESS, expirationDate, null, nextWritePointer, false);
  }

  public static TransactionEdit createCommitting(long writePointer, Set<ChangeId> changes) {
    return new TransactionEdit(writePointer, State.COMMITTING, 0L, changes, 0L, false);
  }

  public static TransactionEdit createCommitted(long writePointer, Set<ChangeId> changes, long nextWritePointer,
                                                boolean canCommit) {
    return new TransactionEdit(writePointer, State.COMMITTED, 0L, changes, nextWritePointer, canCommit);
  }

  public static TransactionEdit createAborted(long writePointer) {
    return new TransactionEdit(writePointer, State.ABORTED, 0L, null, 0L, false);
  }

  public static TransactionEdit createInvalid(long writePointer) {
    return new TransactionEdit(writePointer, State.INVALID, 0L, null, 0L, false);
  }

  public static TransactionEdit createMoveWatermark(long writePointer) {
    return new TransactionEdit(writePointer, State.MOVE_WATERMARK, 0L, null, 0L, false);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    out.writeLong(writePointer);
    // use ordinal for predictable size, though this does not support evolution
    out.writeInt(state.ordinal());
    out.writeLong(expirationDate);
    out.writeLong(nextWritePointer);
    out.writeBoolean(canCommit);
    if (changes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(changes.size());
      for (ChangeId c : changes) {
        byte[] cKey = c.getKey();
        out.writeInt(cKey.length);
        out.write(cKey);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (changes == null) {
      changes = Sets.newHashSet();
    } else {
      changes.clear();
    }

    byte version = in.readByte();
    if (version != VERSION) {
      throw new IOException("Unexpected version for edit!");
    }
    this.writePointer = in.readLong();
    int stateIdx = in.readInt();
    try {
      state = TransactionEdit.State.values()[stateIdx];
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IOException("State enum ordinal value is out of range: " + stateIdx);
    }
    expirationDate = in.readLong();
    nextWritePointer = in.readLong();
    canCommit = in.readBoolean();
    int changeSize = in.readInt();
    for (int i = 0; i < changeSize; i++) {
      int currentLength = in.readInt();
      byte[] currentBytes = new byte[currentLength];
      in.readFully(currentBytes);
      changes.add(new ChangeId(currentBytes));
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TransactionEdit)) {
      return false;
    }
    TransactionEdit other = (TransactionEdit) obj;
    return Objects.equal(writePointer, other.writePointer) &&
      Objects.equal(nextWritePointer, other.nextWritePointer) &&
      Objects.equal(expirationDate, other.expirationDate) &&
      Objects.equal(state, other.state) &&
      Objects.equal(changes, other.changes) &&
      Objects.equal(canCommit, other.canCommit);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("writePointer", writePointer)
      .add("nextWritePointer", nextWritePointer)
      .add("expiration", expirationDate)
      .add("state", state)
      .add("changesSize", changes != null ? changes.size() : 0)
      .add("canCommit", canCommit)
      .toString();
  }
}
