package com.continuuity.data2.transaction;

import com.google.common.base.Objects;
import com.google.gson.Gson;

import java.util.Arrays;

/**
 *
 */
public class Transaction {
  private final long readPointer;
  private final long writePointer;
  private final long[] invalids;
  private final long[] inProgress;

  private static final Gson gson = new Gson();

  private static final long[] NO_EXCLUDES = { };

  public static final Transaction ALL_VISIBLE_LATEST =
    new Transaction(Long.MAX_VALUE, Long.MAX_VALUE, NO_EXCLUDES, NO_EXCLUDES);

  public Transaction(long readPointer, long writePointer, long[] invalids, long[] inProgress) {
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.invalids = invalids;
    this.inProgress = inProgress;
  }

  public long getReadPointer() {
    return readPointer;
  }

  public long getWritePointer() {
    return writePointer;
  }

  public long[] getInvalids() {
    return invalids;
  }

  public long[] getInProgress() {
    return inProgress;
  }

  public long getFirstInProgress() {
    return inProgress.length == 0 ? Long.MAX_VALUE : inProgress[0];
  }

  public boolean isInProgress(long version) {
    return Arrays.binarySearch(inProgress, version) >= 0;
  }

  public boolean isExcluded(long version) {
    return Arrays.binarySearch(inProgress, version) >= 0
      || Arrays.binarySearch(invalids, version) >= 0;
  }

  public boolean isVisible(long version) {
    return version <= getReadPointer() && !isExcluded(version);
  }

  public boolean hasExcludes() {
    return invalids.length > 0 || inProgress.length > 0;
  }


  public int excludesSize() {
    return invalids.length + inProgress.length;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static Transaction fromJson(String json) {
    return gson.fromJson(json, Transaction.class);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("readPointer", readPointer)
                  .add("writePointer", writePointer)
                  .add("invalids", Arrays.toString(invalids))
                  .add("inProgress", Arrays.toString(inProgress))
                  .toString();
  }
}
