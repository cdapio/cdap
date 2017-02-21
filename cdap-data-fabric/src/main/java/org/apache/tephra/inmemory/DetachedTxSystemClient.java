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

package org.apache.tephra.inmemory;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of the tx system client that doesn't talk to any global service and tries to do its best to meet the
 * tx system requirements/expectations. In fact it implements enough logic to support running flows (when each flowlet
 * uses its own detached tx system client, without talking to each other and sharing any state) with "process exactly
 * once" guarantee if no failures happen.
 *
 * NOTE: Will NOT detect conflicts. May leave inconsistent state when process crashes. Does NOT provide even read
 *       isolation guarantees.
 *
 * Good for performance testing. For demoing high throughput. For use-cases with relaxed tx guarantees.
 */
public class DetachedTxSystemClient implements TransactionSystemClient {
  // Dataset and queue logic relies on tx id to grow monotonically even after restart. Hence we need to start with
  // value that is for sure bigger than the last one used before restart.
  // NOTE: with code below we assume we don't do more than InMemoryTransactionManager.MAX_TX_PER_MS tx/ms
  //       by single client
  private AtomicLong generator = new AtomicLong(System.currentTimeMillis() * TxConstants.MAX_TX_PER_MS);

  @Override
  public Transaction startShort() {
    long wp = getWritePointer();
    // NOTE: -1 here is because we have logic that uses (readpointer + 1) as a "exclusive stop key" in some datasets
    return new Transaction(
      Long.MAX_VALUE - 1, wp, new long[0], new long[0],
      Transaction.NO_TX_IN_PROGRESS, TransactionType.SHORT);
  }

  private long getWritePointer() {
    long wp = generator.incrementAndGet();
    // NOTE: using InMemoryTransactionManager.MAX_TX_PER_MS to be at least close to real one
    long now = System.currentTimeMillis();
    if (wp < now * TxConstants.MAX_TX_PER_MS) {
      // trying to advance to align with timestamp, but only once: if failed, we'll just try again later with next tx
      long advanced = now * TxConstants.MAX_TX_PER_MS;
      if (generator.compareAndSet(wp, advanced)) {
        wp = advanced;
      }
    }
    return wp;
  }

  @Override
  public Transaction startShort(int timeout) {
    return startShort();
  }

  @Override
  public Transaction startLong() {
    return startShort();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    return true;
  }

  @Override
  public boolean commit(Transaction tx) {
    return true;
  }

  @Override
  public void abort(Transaction tx) {
    // do nothing
  }

  @Override
  public boolean invalidate(long tx) {
    return true;
  }

  @Override
  public Transaction checkpoint(Transaction tx) {
    long newWritePointer = getWritePointer();
    LongArrayList newCheckpointPointers = new LongArrayList(tx.getCheckpointWritePointers());
    newCheckpointPointers.add(newWritePointer);
    return new Transaction(tx, newWritePointer, newCheckpointPointers.toLongArray());
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    throw new TransactionCouldNotTakeSnapshotException(
        "Snapshot not implemented in detached transaction system client");
  }

  @Override
  public String status() {
    return TxConstants.STATUS_OK;
  }

  @Override
  public void resetState() {
    // do nothing
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return true;
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return true;
  }

  @Override
  public int getInvalidSize() {
    return 0;
  }
}
