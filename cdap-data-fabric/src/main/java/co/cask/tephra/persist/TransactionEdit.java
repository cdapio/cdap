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

package co.cask.tephra.persist;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.Writable;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionType;
import org.apache.tephra.persist.TransactionLog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Represents a transaction state change in the {@link TransactionLog}.
 * This class was included for backward compatibility reasons. It will be removed in future releases.
 */
@Deprecated
public class TransactionEdit implements Writable {

  /**
   * The possible state changes for a transaction.
   */
  public enum State {
    INPROGRESS, COMMITTING, COMMITTED, INVALID, ABORTED, MOVE_WATERMARK, TRUNCATE_INVALID_TX, CHECKPOINT
  }

  private long writePointer;

  /**
   * stores the value of visibility upper bound
   * (see {@link TransactionManager.InProgressTx#getVisibilityUpperBound()})
   * for edit of {@link State#INPROGRESS} only
   */
  private long visibilityUpperBound;
  private long commitPointer;
  private long expirationDate;
  private State state;
  private Set<ChangeId> changes;
  /** Whether or not the COMMITTED change should be fully committed. */
  private boolean canCommit;
  private TransactionType type;
  private Set<Long> truncateInvalidTx;
  private long truncateInvalidTxTime;
  private long parentWritePointer;
  private long[] checkpointPointers;

  // for Writable
  public TransactionEdit() {
    this.changes = Sets.newHashSet();
    this.truncateInvalidTx = Sets.newHashSet();
  }

  // package private for testing
  TransactionEdit(long writePointer, long visibilityUpperBound, State state, long expirationDate,
                  Set<ChangeId> changes, long commitPointer, boolean canCommit, TransactionType type,
                  Set<Long> truncateInvalidTx, long truncateInvalidTxTime, long parentWritePointer,
                  long[] checkpointPointers) {
    this.writePointer = writePointer;
    this.visibilityUpperBound = visibilityUpperBound;
    this.state = state;
    this.expirationDate = expirationDate;
    this.changes = changes != null ? changes : Collections.<ChangeId>emptySet();
    this.commitPointer = commitPointer;
    this.canCommit = canCommit;
    this.type = type;
    this.truncateInvalidTx = truncateInvalidTx != null ? truncateInvalidTx : Collections.<Long>emptySet();
    this.truncateInvalidTxTime = truncateInvalidTxTime;
    this.parentWritePointer = parentWritePointer;
    this.checkpointPointers = checkpointPointers;
  }

  /**
   * Returns the transaction write pointer assigned for the state change.
   */
  public long getWritePointer() {
    return writePointer;
  }

  void setWritePointer(long writePointer) {
    this.writePointer = writePointer;
  }

  public long getVisibilityUpperBound() {
    return visibilityUpperBound;
  }

  void setVisibilityUpperBound(long visibilityUpperBound) {
    this.visibilityUpperBound = visibilityUpperBound;
  }

  /**
   * Returns the type of state change represented.
   */
  public State getState() {
    return state;
  }

  void setState(State state) {
    this.state = state;
  }

  /**
   * Returns any expiration timestamp (in milliseconds) associated with the state change.  This should only
   * be populated for changes of type {@link State#INPROGRESS}.
   */
  public long getExpiration() {
    return expirationDate;
  }

  void setExpiration(long expirationDate) {
    this.expirationDate = expirationDate;
  }

  /**
   * @return the set of changed row keys associated with the state change.  This is only populated for edits
   * of type {@link State#COMMITTING} or {@link State#COMMITTED}.
   */
  public Set<ChangeId> getChanges() {
    return changes;
  }

  void setChanges(Set<ChangeId> changes) {
    this.changes = changes;
  }

  /**
   * Returns the write pointer used to commit the row key change set.  This is only populated for edits of type
   * {@link State#COMMITTED}.
   */
  public long getCommitPointer() {
    return commitPointer;
  }

  void setCommitPointer(long commitPointer) {
    this.commitPointer = commitPointer;
  }

  /**
   * Returns whether or not the transaction should be moved to the committed set.  This is only populated for edits
   * of type {@link State#COMMITTED}.
   */
  public boolean getCanCommit() {
    return canCommit;
  }

  void setCanCommit(boolean canCommit) {
    this.canCommit = canCommit;
  }

  /**
   * Returns the transaction type. This is only populated for edits of type {@link State#INPROGRESS} or
   * {@link State#ABORTED}.
   */
  public TransactionType getType() {
    return type;
  }

  void setType(TransactionType type) {
    this.type = type;
  }

  /**
   * Returns the transaction ids to be removed from invalid transaction list. This is only populated for
   * edits of type {@link State#TRUNCATE_INVALID_TX}
   */
  public Set<Long> getTruncateInvalidTx() {
    return truncateInvalidTx;
  }

  void setTruncateInvalidTx(Set<Long> truncateInvalidTx) {
    this.truncateInvalidTx = truncateInvalidTx;
  }

  /**
   * Returns the time until which the invalid transactions need to be truncated from invalid transaction list.
   * This is only populated for  edits of type {@link State#TRUNCATE_INVALID_TX}
   */
  public long getTruncateInvalidTxTime() {
    return truncateInvalidTxTime;
  }

  void setTruncateInvalidTxTime(long truncateInvalidTxTime) {
    this.truncateInvalidTxTime = truncateInvalidTxTime;
  }

  /**
   * Returns the parent write pointer for a checkpoint operation.  This is only populated for edits of type
   * {@link State#CHECKPOINT}
   */
  public long getParentWritePointer() {
    return parentWritePointer;
  }

  void setParentWritePointer(long parentWritePointer) {
    this.parentWritePointer = parentWritePointer;
  }

  /**
   * Returns the checkpoint write pointers for the edit.  This is only populated for edits of type
   * {@link State#ABORTED}.
   */
  public long[] getCheckpointPointers() {
    return checkpointPointers;
  }

  void setCheckpointPointers(long[] checkpointPointers) {
    this.checkpointPointers = checkpointPointers;
  }

  /**
   * Creates a new instance in the {@link State#INPROGRESS} state.
   */
  public static TransactionEdit createStarted(long writePointer, long visibilityUpperBound,
                                              long expirationDate, TransactionType type) {
    return new TransactionEdit(writePointer, visibilityUpperBound, State.INPROGRESS,
                               expirationDate, null, 0L, false, type, null, 0L, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#COMMITTING} state.
   */
  public static TransactionEdit createCommitting(long writePointer, Set<ChangeId> changes) {
    return new TransactionEdit(writePointer, 0L, State.COMMITTING, 0L, changes, 0L, false, null, null, 0L, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#COMMITTED} state.
   */
  public static TransactionEdit createCommitted(long writePointer, Set<ChangeId> changes, long nextWritePointer,
                                                boolean canCommit) {
    return new TransactionEdit(writePointer, 0L, State.COMMITTED, 0L, changes, nextWritePointer, canCommit, null,
                               null, 0L, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#ABORTED} state.
   */
  public static TransactionEdit createAborted(long writePointer, TransactionType type, long[] checkpointPointers) {
    return new TransactionEdit(writePointer, 0L, State.ABORTED, 0L, null, 0L, false, type, null, 0L, 0L,
                               checkpointPointers);
  }

  /**
   * Creates a new instance in the {@link State#INVALID} state.
   */
  public static TransactionEdit createInvalid(long writePointer) {
    return new TransactionEdit(writePointer, 0L, State.INVALID, 0L, null, 0L, false, null, null, 0L, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#MOVE_WATERMARK} state.
   */
  public static TransactionEdit createMoveWatermark(long writePointer) {
    return new TransactionEdit(writePointer, 0L, State.MOVE_WATERMARK, 0L, null, 0L, false, null, null, 0L, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#TRUNCATE_INVALID_TX} state.
   */
  public static TransactionEdit createTruncateInvalidTx(Set<Long> truncateInvalidTx) {
    return new TransactionEdit(0L, 0L, State.TRUNCATE_INVALID_TX, 0L, null, 0L, false, null, truncateInvalidTx,
                               0L, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#TRUNCATE_INVALID_TX} state.
   */
  public static TransactionEdit createTruncateInvalidTxBefore(long truncateInvalidTxTime) {
    return new TransactionEdit(0L, 0L, State.TRUNCATE_INVALID_TX, 0L, null, 0L, false, null, null,
                               truncateInvalidTxTime, 0L, null);
  }

  /**
   * Creates a new instance in the {@link State#CHECKPOINT} state.
   */
  public static TransactionEdit createCheckpoint(long writePointer, long parentWritePointer) {
    return new TransactionEdit(writePointer, 0L, State.CHECKPOINT, 0L, null, 0L, false, null, null, 0L,
                               parentWritePointer, null);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TransactionEditCodecs.encode(this, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    TransactionEditCodecs.decode(this, in);
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TransactionEdit)) {
      return false;
    }

    TransactionEdit that = (TransactionEdit) o;

    return Objects.equal(this.writePointer, that.writePointer) &&
      Objects.equal(this.visibilityUpperBound, that.visibilityUpperBound) &&
      Objects.equal(this.commitPointer, that.commitPointer) &&
      Objects.equal(this.expirationDate, that.expirationDate) &&
      Objects.equal(this.state, that.state) &&
      Objects.equal(this.changes, that.changes) &&
      Objects.equal(this.canCommit, that.canCommit) &&
      Objects.equal(this.type, that.type) &&
      Objects.equal(this.truncateInvalidTx, that.truncateInvalidTx) &&
      Objects.equal(this.truncateInvalidTxTime, that.truncateInvalidTxTime) &&
      Objects.equal(this.parentWritePointer, that.parentWritePointer) &&
      Arrays.equals(this.checkpointPointers, that.checkpointPointers);
  }

  @Override
  public final int hashCode() {
    return Objects.hashCode(writePointer, visibilityUpperBound, commitPointer, expirationDate, state, changes,
                            canCommit, type, parentWritePointer, checkpointPointers);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("writePointer", writePointer)
      .add("visibilityUpperBound", visibilityUpperBound)
      .add("commitPointer", commitPointer)
      .add("expiration", expirationDate)
      .add("state", state)
      .add("changesSize", changes != null ? changes.size() : 0)
      .add("canCommit", canCommit)
      .add("type", type)
      .add("truncateInvalidTx", truncateInvalidTx)
      .add("truncateInvalidTxTime", truncateInvalidTxTime)
      .add("parentWritePointer", parentWritePointer)
      .add("checkpointPointers", checkpointPointers)
      .toString();
  }

}

