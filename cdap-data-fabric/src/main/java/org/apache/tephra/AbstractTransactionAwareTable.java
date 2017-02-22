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

package org.apache.tephra;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Base class for all the common parts of the HBase version-specific {@code TransactionAwareHTable}
 * implementations.
 */
public abstract class AbstractTransactionAwareTable implements TransactionAware {
  protected final TransactionCodec txCodec;
  // map of write pointers to change set associated with each
  protected final Map<Long, Set<ActionChange>> changeSets;
  protected final TxConstants.ConflictDetection conflictLevel;
  protected Transaction tx;
  protected boolean allowNonTransactional;

  public AbstractTransactionAwareTable(TxConstants.ConflictDetection conflictLevel, boolean allowNonTransactional) {
    this.conflictLevel = conflictLevel;
    this.allowNonTransactional = allowNonTransactional;
    this.txCodec = new TransactionCodec();
    this.changeSets = Maps.newHashMap();
  }

  /**
   * True if the instance allows non-transaction operations.
   * @return
   */
  public boolean getAllowNonTransactional() {
    return this.allowNonTransactional;
  }

  /**
   * Set whether the instance allows non-transactional operations.
   * @param allowNonTransactional
   */
  public void setAllowNonTransactional(boolean allowNonTransactional) {
    this.allowNonTransactional = allowNonTransactional;
  }

  @Override
  public void startTx(Transaction tx) {
    this.tx = tx;
  }

  @Override
  public void updateTx(Transaction tx) {
    this.tx = tx;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    if (conflictLevel == TxConstants.ConflictDetection.NONE) {
      return Collections.emptyList();
    }

    Collection<byte[]> txChanges = new TreeSet<byte[]>(UnsignedBytes.lexicographicalComparator());
    for (Set<ActionChange> changeSet : changeSets.values()) {
      for (ActionChange change : changeSet) {
        txChanges.add(getChangeKey(change.getRow(), change.getFamily(), change.getQualifier()));
      }
    }
    return txChanges;
  }

  public byte[] getChangeKey(byte[] row, byte[] family, byte[] qualifier) {
    byte[] key;
    switch (conflictLevel) {
      case ROW:
        key = Bytes.concat(getTableKey(), row);
        break;
      case COLUMN:
        key = Bytes.concat(getTableKey(), row, family, qualifier);
        break;
      case NONE:
        throw new IllegalStateException("NONE conflict detection does not support change keys");
      default:
        throw new IllegalStateException("Unknown conflict detection level: " + conflictLevel);
    }
    return key;
  }

  @Override
  public boolean commitTx() throws Exception {
    return doCommit();
  }

  /**
   * Commits any pending writes by flushing the wrapped {@code HTable} instance.
   */
  protected abstract boolean doCommit() throws IOException;

  @Override
  public void postTxCommit() {
    tx = null;
    changeSets.clear();
  }

  @Override
  public String getTransactionAwareName() {
    return new String(getTableKey(), Charsets.UTF_8);
  }

  /**
   * Returns the table name to use as a key prefix for the transaction change set.
   */
  protected abstract byte[] getTableKey();

  @Override
  public boolean rollbackTx() throws Exception {
    return doRollback();
  }

  /**
   * Rolls back any persisted changes from the transaction by issuing offsetting deletes to the
   * wrapped {@code HTable} instance.  How this is handled will depend on the delete API exposed
   * by the specific version of HBase.
   */
  protected abstract boolean doRollback() throws Exception;

  protected void addToChangeSet(byte[] row, byte[] family, byte[] qualifier) {
    long currentWritePointer = tx.getWritePointer();
    Set<ActionChange> changeSet = changeSets.get(currentWritePointer);
    if (changeSet == null) {
      changeSet = Sets.newHashSet();
      changeSets.put(currentWritePointer, changeSet);
    }
    switch (conflictLevel) {
      case ROW:
      case NONE:
        // with ROW or NONE conflict detection, we still need to track changes per-family, since this
        // is the granularity at which we will issue deletes for rollback
        changeSet.add(new ActionChange(row, family));
        break;
      case COLUMN:
        changeSet.add(new ActionChange(row, family, qualifier));
        break;
      default:
        throw new IllegalStateException("Unknown conflict detection level: " + conflictLevel);
    }
  }

  /**
   * Record of each transaction that causes a change. This reference is used to rollback
   * any operation upon failure.
   */
  protected class ActionChange {
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;

    public ActionChange(byte[] row, byte[] family) {
      this(row, family, null);
    }

    public ActionChange(byte[] row, byte[] family, byte[] qualifier) {
      this.row = row;
      this.family = family;
      this.qualifier = qualifier;
    }

    public byte[] getRow() {
      return row;
    }

    public byte[] getFamily() {
      return family;
    }

    public byte[] getQualifier() {
      return qualifier;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != this.getClass()) {
        return false;
      }

      if (o == this) {
        return true;
      }

      ActionChange other = (ActionChange) o;
      return Objects.equal(this.row, other.row) &&
             Objects.equal(this.family, other.family) &&
             Objects.equal(this.qualifier, other.qualifier);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(row);
      result = 31 * result + (family != null ? Arrays.hashCode(family) : 0);
      result = 31 * result + (qualifier != null ? Arrays.hashCode(qualifier) : 0);
      return result;
    }
  }
}
