package com.continuuity.data2.transaction;

import java.util.Arrays;

/**
 *
 */
public class Transaction {
  private long readPointer;
  private long writePointer;
  private long[] excludedList;

  public Transaction(long readPointer, long writePointer, long[] excludedList) {
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.excludedList = excludedList;
  }

  public long getReadPointer() {
    return readPointer;
  }

  public long getWritePointer() {
    return writePointer;
  }

  // todo: these are ordered
  public long[] getExcludedList() {
    return excludedList;
  }

  public boolean isVisible(long version) {
    return version <= getReadPointer() && Arrays.binarySearch(getExcludedList(), version) < 0;
  }
}
