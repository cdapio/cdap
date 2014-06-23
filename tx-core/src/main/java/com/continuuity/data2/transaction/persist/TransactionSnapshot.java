package com.continuuity.data2.transaction.persist;

import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Represents an in-memory snapshot of the full transaction state.
 */
public class TransactionSnapshot {
  private long timestamp;
  private long readPointer;
  private long writePointer;
  private Collection<Long> invalid;
  private NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress;
  private Map<Long, Set<ChangeId>> committingChangeSets;
  private Map<Long, Set<ChangeId>> committedChangeSets;

  public TransactionSnapshot(long timestamp, long readPointer, long writePointer, Collection<Long> invalid,
                             NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress,
                             Map<Long, Set<ChangeId>> committing, Map<Long, Set<ChangeId>> committed) {
    this.timestamp = timestamp;
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.invalid = invalid;
    this.inProgress = inProgress;
    this.committingChangeSets = committing;
    this.committedChangeSets = committed;
  }

  /**
   * Returns the timestamp from when this snapshot was created.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the read pointer at the time of the snapshot.
   */
  public long getReadPointer() {
    return readPointer;
  }

  /**
   * Returns the next write pointer at the time of the snapshot.
   */
  public long getWritePointer() {
    return writePointer;
  }

  /**
   * Returns the list of invalid write pointers at the time of the snapshot.
   */
  public Collection<Long> getInvalid() {
    return invalid;
  }

  /**
   * Returns the map of in-progress transaction write pointers at the time of the snapshot.
   * @return a map of write pointer to expiration timestamp (in milliseconds) for all transactions in-progress.
   */
  public Map<Long, InMemoryTransactionManager.InProgressTx> getInProgress() {
    return inProgress;
  }

  /**
   * Returns a map of transaction write pointer to sets of changed row keys for transactions that had called
   * {@code InMemoryTransactionManager.canCommit(Transaction, Collection)} but not yet called
   * {@code InMemoryTransactionManager.commit(Transaction)} at the time of the snapshot.
   *
   * @return a map of transaction write pointer to set of changed row keys.
   */
  public Map<Long, Set<ChangeId>> getCommittingChangeSets() {
    return committingChangeSets;
  }

  /**
   * Returns a map of transaction write pointer to set of changed row keys for transaction that had successfully called
   * {@code InMemoryTransactionManager.commit(Transaction)} at the time of the snapshot.
   *
   * @return a map of transaction write pointer to set of changed row keys.
   */
  public Map<Long, Set<ChangeId>> getCommittedChangeSets() {
    return committedChangeSets;
  }

  /**
   * @return transaction id {@code X} such that any of the transactions newer than {@code X} might be invisible to
   *         some of the currently in-progress transactions or to those that will be started <p>
   *         NOTE: the returned tx id can be invalid.
   */
  public long getVisibilityUpperBound() {
    // the readPointer of the oldest in-progress tx is the oldest in use
    // todo: potential problem with not moving visibility upper bound for the whole duration of long-running tx
    Map.Entry<Long, InMemoryTransactionManager.InProgressTx> firstInProgress = inProgress.firstEntry();
    if (firstInProgress == null) {
      // using readPointer as smallest visible when non txs are there
      return readPointer;
    }
    return firstInProgress.getValue().getVisibilityUpperBound();
  }

  /**
   * Checks that this instance matches another {@code TransactionSnapshot} instance.  Note that the equality check
   * ignores the snapshot timestamp value, but includes all other properties.
   *
   * @param obj the other instance to check for equality.
   * @return {@code true} if the instances are equal, {@code false} if not.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof  TransactionSnapshot)) {
      return false;
    }
    TransactionSnapshot other = (TransactionSnapshot) obj;
    return readPointer == other.readPointer &&
      writePointer == other.writePointer &&
      invalid.equals(other.invalid) &&
      inProgress.equals(other.inProgress) &&
      committingChangeSets.equals(other.committingChangeSets) &&
      committedChangeSets.equals(other.committedChangeSets);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("timestamp", timestamp)
        .add("readPointer", readPointer)
        .add("writePointer", writePointer)
        .add("invalidSize", invalid.size())
        .add("inProgressSize", inProgress.size())
        .add("committingSize", committingChangeSets.size())
        .add("committedSize", committedChangeSets.size())
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(readPointer, writePointer, invalid, inProgress, committingChangeSets, committedChangeSets);
  }

  /**
   * Creates a new {@code TransactionSnapshot} instance with copies of all of the individual collections.
   * @param readPointer current transaction read pointer
   * @param writePointer current transaction write pointer
   * @param invalid current list of invalid write pointers
   * @param inProgress current map of in-progress write pointers to expiration timestamps
   * @param committing current map of write pointers to change sets which have passed {@code canCommit()} but not
   *                   yet committed
   * @param committed current map of write pointers to change sets which have committed
   * @return a new {@code TransactionSnapshot} instance
   */
  public static TransactionSnapshot copyFrom(long snapshotTime, long readPointer,
                                             long writePointer, Collection<Long> invalid,
                                             NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress,
                                             Map<Long, Set<ChangeId>> committing,
                                             NavigableMap<Long, Set<ChangeId>> committed) {
    // copy invalid IDs
    Collection<Long> invalidCopy = Lists.newArrayList(invalid);
    // copy in-progress IDs and expirations
    NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgressCopy = Maps.newTreeMap(inProgress);

    // for committing and committed maps, we need to copy each individual Set as well to prevent modification
    Map<Long, Set<ChangeId>> committingCopy = Maps.newHashMap();
    for (Map.Entry<Long, Set<ChangeId>> entry : committing.entrySet()) {
      committingCopy.put(entry.getKey(), new HashSet<ChangeId>(entry.getValue()));
    }

    NavigableMap<Long, Set<ChangeId>> committedCopy = new TreeMap<Long, Set<ChangeId>>();
    for (Map.Entry<Long, Set<ChangeId>> entry : committed.entrySet()) {
      committedCopy.put(entry.getKey(), new HashSet<ChangeId>(entry.getValue()));
    }

    return new TransactionSnapshot(snapshotTime, readPointer, writePointer,
                                   invalidCopy, inProgressCopy, committingCopy, committedCopy);
  }
}
