/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction;

import co.cask.tephra.InvalidTruncateTimeException;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionCouldNotTakeSnapshotException;
import co.cask.tephra.TransactionNotInProgressException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;

/**
 * Delegates all transaction methods to a TransactionSystemClient and does nothing during startup and shutdown.
 */
public class DelegatingTransactionSystemClientService
  extends AbstractIdleService implements TransactionSystemClientService {

  private final TransactionSystemClient delegate;

  @Inject
  public DelegatingTransactionSystemClientService(TransactionSystemClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public Transaction startShort() {
    return delegate.startShort();
  }

  @Override
  public Transaction startShort(int timeout) {
    return delegate.startShort(timeout);
  }

  @Override
  public Transaction startLong() {
    return delegate.startLong();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return delegate.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    return delegate.commit(tx);
  }

  @Override
  public void abort(Transaction tx) {
    delegate.abort(tx);
  }

  @Override
  public boolean invalidate(long tx) {
    return delegate.invalidate(tx);
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    return delegate.checkpoint(tx);
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    return delegate.getSnapshotInputStream();
  }

  @Override
  public String status() {
    return delegate.status();
  }

  @Override
  public void resetState() {
    delegate.resetState();
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return delegate.truncateInvalidTx(invalidTxIds);
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return delegate.truncateInvalidTxBefore(time);
  }

  @Override
  public int getInvalidSize() {
    return delegate.getInvalidSize();
  }

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }
}
