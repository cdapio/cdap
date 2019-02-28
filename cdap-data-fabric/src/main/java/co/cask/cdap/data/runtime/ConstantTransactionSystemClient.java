/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data.runtime;

import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 * An implementation of {@link TransactionSystemClient} that doesn't provide read isolation.
 * This class is a temporary workaround for transactional operations in environments that doesn't really support/need
 * transaction, until we can remove the strong dependency on Transaction in the runtime system.
 */
public class ConstantTransactionSystemClient implements TransactionSystemClient {

  @Override
  public Transaction startShort() {
    return new Transaction(Long.MAX_VALUE - 1, System.currentTimeMillis() * TxConstants.MAX_TX_PER_MS,
                           new long[0], new long[0], Transaction.NO_TX_IN_PROGRESS, TransactionType.SHORT);
  }

  @Override
  public Transaction startShort(int timeout) {
    return startShort();
  }

  @Override
  public Transaction startLong() {
    return new Transaction(Long.MAX_VALUE - 1, System.currentTimeMillis() * TxConstants.MAX_TX_PER_MS,
                           new long[0], new long[0], Transaction.NO_TX_IN_PROGRESS, TransactionType.LONG);
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    return true;
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) {
    // no-op
  }

  @Override
  public boolean commit(Transaction tx) {
    return true;
  }

  @Override
  public void commitOrThrow(Transaction tx) {
    // no-op
  }

  @Override
  public void abort(Transaction tx) {
    // no-op
  }

  @Override
  public boolean invalidate(long tx) {
    return true;
  }

  @Override
  public Transaction checkpoint(Transaction tx) {
    return tx;
  }

  @Override
  public InputStream getSnapshotInputStream() {
    throw new UnsupportedOperationException("getSnapshotInputStream is not supported");
  }

  @Override
  public String status() {
    return TxConstants.STATUS_OK;
  }

  @Override
  public void resetState() {
    // no-op
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return true;
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) {
    return true;
  }

  @Override
  public int getInvalidSize() {
    return 0;
  }

  @Override
  public void pruneNow() {
    // no-op
  }
}
