package com.continuuity.data.operation.executor;

import com.google.common.base.Objects;

/**
 * This represents a transaction in Omid. It has a transaction id, used as the write timestamp,
 * and a read pointer that provides read (snapshot) isolation.
 */
public final class Transaction implements ReadPointer, WritePointer {
  private final WritePointer writePointer;
  private final ReadPointer readPointer;
  private final boolean trackChanges;

  @Deprecated
  public Transaction(WritePointer writePointer, ReadPointer readPointer) {
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.trackChanges = true;
  }

  public Transaction(final long writeVersion, ReadPointer readPointer, boolean trackChanges) {
    this.writePointer = new WritePointer() {
      @Override
      public long getWriteVersion() {
        return writeVersion;
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
          .add("writeVersion", getWriteVersion())
          .toString();
      }
    };
    this.readPointer = readPointer;
    this.trackChanges = trackChanges;
  }

  public ReadPointer getReadPointer() {
    return readPointer;
  }

  @Override
  public boolean isVisible(long txid) {
    return readPointer.isVisible(txid);
  }

  @Override
  public long getMaximum() {
    return readPointer.getMaximum();
  }

  @Override
  public long getWriteVersion() {
    return writePointer.getWriteVersion();
  }

  public boolean isTrackChanges() {
    return trackChanges;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("writePointer", writePointer)
      .add("readPointer", readPointer)
      .toString();
  }
}
