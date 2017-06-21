/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.persist;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Represents an in-memory snapshot of the full transaction state.
 */
public class TransactionSnapshot implements TransactionVisibilityState {
  private long timestamp;
  private long readPointer;
  private long writePointer;
  private Collection<Long> invalid;
  private NavigableMap<Long, TransactionManager.InProgressTx> inProgress;
  private Map<Long, Set<ChangeId>> committingChangeSets;
  private Map<Long, Set<ChangeId>> committedChangeSets;

  public TransactionSnapshot(long timestamp, long readPointer, long writePointer, Collection<Long> invalid,
                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress,
                             Map<Long, Set<ChangeId>> committing, Map<Long, Set<ChangeId>> committed) {
    this(timestamp, readPointer, writePointer, invalid, inProgress);
    this.committingChangeSets = committing;
    this.committedChangeSets = committed;
  }

  public TransactionSnapshot(long timestamp, long readPointer, long writePointer, Collection<Long> invalid,
                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress) {
    this.timestamp = timestamp;
    this.readPointer = readPointer;
    this.writePointer = writePointer;
    this.invalid = invalid;
    this.inProgress = inProgress;
    this.committingChangeSets = Collections.emptyMap();
    this.committedChangeSets = Collections.emptyMap();
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public long getReadPointer() {
    return readPointer;
  }

  @Override
  public long getWritePointer() {
    return writePointer;
  }

  @Override
  public Collection<Long> getInvalid() {
    return invalid;
  }

  @Override
  public NavigableMap<Long, TransactionManager.InProgressTx> getInProgress() {
    return inProgress;
  }

  @Override
  public long getVisibilityUpperBound() {
    // the readPointer of the oldest in-progress tx is the oldest in use
    // todo: potential problem with not moving visibility upper bound for the whole duration of long-running tx
    Map.Entry<Long, TransactionManager.InProgressTx> firstInProgress = inProgress.firstEntry();
    if (firstInProgress == null) {
      // using readPointer as smallest visible when non txs are there
      return readPointer;
    }
    return firstInProgress.getValue().getVisibilityUpperBound();
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
                                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress,
                                             Map<Long, Set<ChangeId>> committing,
                                             NavigableMap<Long, Set<ChangeId>> committed) {
    // copy invalid IDs
    Collection<Long> invalidCopy = Lists.newArrayList(invalid);
    // copy in-progress IDs and expirations
    NavigableMap<Long, TransactionManager.InProgressTx> inProgressCopy = Maps.newTreeMap(inProgress);

    // for committing and committed maps, we need to copy each individual Set as well to prevent modification
    Map<Long, Set<ChangeId>> committingCopy = Maps.newHashMap();
    for (Map.Entry<Long, Set<ChangeId>> entry : committing.entrySet()) {
      committingCopy.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    NavigableMap<Long, Set<ChangeId>> committedCopy = new TreeMap<>();
    for (Map.Entry<Long, Set<ChangeId>> entry : committed.entrySet()) {
      committedCopy.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }

    return new TransactionSnapshot(snapshotTime, readPointer, writePointer,
                                   invalidCopy, inProgressCopy, committingCopy, committedCopy);
  }

}
