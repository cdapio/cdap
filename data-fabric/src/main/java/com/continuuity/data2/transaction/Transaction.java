package com.continuuity.data2.transaction;

import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * Transaction details
 */
// NOTE: this class should have minimal dependencies as it is used in HBase CPs and other places where minimal classes
//       are available
public class Transaction {
  private final long readPointer;
  private final long writePointer;
  private final long[] invalids;
  private final long[] inProgress;
  private final long firstShortInProgress;

  private static final long[] NO_EXCLUDES = { };
  public static final long NO_TX_IN_PROGRESS = Long.MAX_VALUE;

  public static final Transaction ALL_VISIBLE_LATEST =
    new Transaction(Long.MAX_VALUE, Long.MAX_VALUE, NO_EXCLUDES, NO_EXCLUDES, NO_TX_IN_PROGRESS);

  public Transaction(long readPointer, long writePointer, long[] invalids, long[] inProgress,
                     long firstShortInProgress) {
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.invalids = invalids;
    this.inProgress = inProgress;
    this.firstShortInProgress = firstShortInProgress;
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
    return inProgress.length == 0 ? NO_TX_IN_PROGRESS : inProgress[0];
  }

  /**
   * @return transaction id {@code X} such that any of the transactions newer than {@code X} may be invisible to this<p>
   *         NOTE: the returned tx id can be invalid.
   */
  public long getVisibilityUpperBound() {
    // NOTE: in some cases when we do not provide visibility guarantee, we set readPointer to MAX value, but
    //       at same time we don't want that to case cleanup everything as this is used for tx janitor + ttl to see
    //       what can be cleaned up. When non-tx mode is implemented better, we should not need this check
    return inProgress.length == 0 ? Math.min(writePointer - 1, readPointer) : inProgress[0] - 1;
  }

  public long getFirstShortInProgress() {
    return firstShortInProgress;
  }

  public boolean isInProgress(long version) {
    return Arrays.binarySearch(inProgress, version) >= 0;
  }

  public boolean isExcluded(long version) {
    return Arrays.binarySearch(inProgress, version) >= 0
      || Arrays.binarySearch(invalids, version) >= 0;
  }

  public boolean isVisible(long version) {
    // either it was committed before or the change belongs to current tx
    return (version <= getReadPointer() && !isExcluded(version)) || writePointer == version;
  }

  public boolean hasExcludes() {
    return invalids.length > 0 || inProgress.length > 0;
  }


  public int excludesSize() {
    return invalids.length + inProgress.length;
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
