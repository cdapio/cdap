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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.manager.InvalidTxList;

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

  /**
   * Creates an instance of TransactionSnapshot with the given transaction state.
   */
  public TransactionSnapshot(long timestamp, long readPointer, long writePointer,
                             Collection<Long> invalid,
                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress,
                             Map<Long, Set<ChangeId>> committing,
                             Map<Long, Set<ChangeId>> committed) {
    this(timestamp, readPointer, writePointer, invalid, inProgress);
    this.committingChangeSets = committing;
    this.committedChangeSets = committed;
  }

  /**
   * Creates an instance of TransactionSnapshot with the given transaction state.
   */
  public TransactionSnapshot(long timestamp, long readPointer, long writePointer,
                             Collection<Long> invalid,
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
    Map.Entry<Long, TransactionManager.InProgressTx> firstInProgress = inProgress.firstEntry();
    if (firstInProgress == null) {
      return readPointer;
    }
    return firstInProgress.getValue().getVisibilityUpperBound();
  }

  public Map<Long, Set<ChangeId>> getCommittingChangeSets() {
    return committingChangeSets;
  }

  public Map<Long, Set<ChangeId>> getCommittedChangeSets() {
    return committedChangeSets;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TransactionSnapshot)) {
      return false;
    }
    TransactionSnapshot other = (TransactionSnapshot) obj;
    return readPointer == other.readPointer
      && writePointer == other.writePointer
      && invalid.equals(other.invalid)
      && inProgress.equals(other.inProgress)
      && committingChangeSets.equals(other.committingChangeSets)
      && committedChangeSets.equals(other.committedChangeSets);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
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
    return Objects.hashCode(
        readPointer, writePointer, invalid, inProgress, committingChangeSets, committedChangeSets);
  }

  /**
   * Creates a new {@code TransactionSnapshot} instance with copies of all of the individual
   * collections.
   */
  public static TransactionSnapshot copyFrom(long snapshotTime, long readPointer,
                                             long writePointer, InvalidTxList invalidTxList,
                                             NavigableMap<Long, TransactionManager.InProgressTx> inProgress,
                                             Map<Long, TransactionManager.ChangeSet> committing,
                                             NavigableMap<Long, TransactionManager.ChangeSet> committed) {
    Collection<Long> invalidCopy = new LongArrayList(invalidTxList.toSortedArray());
    NavigableMap<Long, TransactionManager.InProgressTx> inProgressCopy = Maps.newTreeMap(inProgress);

    Map<Long, Set<ChangeId>> committingCopy = Maps.newHashMap();
    for (Map.Entry<Long, TransactionManager.ChangeSet> entry : committing.entrySet()) {
      committingCopy.put(entry.getKey(), new HashSet<>(entry.getValue().getChangeIds()));
    }

    NavigableMap<Long, Set<ChangeId>> committedCopy = new TreeMap<>();
    for (Map.Entry<Long, TransactionManager.ChangeSet> entry : committed.entrySet()) {
      committedCopy.put(entry.getKey(), new HashSet<>(entry.getValue().getChangeIds()));
    }

    return new TransactionSnapshot(snapshotTime, readPointer, writePointer,
                                   invalidCopy, inProgressCopy, committingCopy, committedCopy);
  }
}
