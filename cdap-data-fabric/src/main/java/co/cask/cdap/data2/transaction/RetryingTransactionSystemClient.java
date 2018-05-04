/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.thrift.TException;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Base class that helps in retrying certain calls to the transaction system.
 * Unfortunately, this class is tightly coupled with the TransactionServiceClient in Tephra,
 * as it determines a retryable failure by checking if the TransactionSystemClient threw a RuntimeException wrapping
 * a TException, which is what TransactionServiceClient does. Once Tephra implements better exceptions, this class
 * can be removed.
 *
 * By default, it doesn't retry anything. Subclasses should override the methods that should be retried.
 */
public abstract class RetryingTransactionSystemClient implements TransactionSystemClient {
  private static final Predicate<Throwable> IS_RETRYABLE = new Predicate<Throwable>() {
    @Override
    public boolean test(Throwable input) {
      return input instanceof RuntimeException && input.getCause() != null && input.getCause() instanceof TException;
    }
  };
  protected final TransactionSystemClient delegate;
  private final RetryStrategy retryStrategy;

  protected RetryingTransactionSystemClient(TransactionSystemClient delegate, RetryStrategy retryStrategy) {
    this.delegate = delegate;
    this.retryStrategy = retryStrategy;
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
    //noinspection deprecation
    return delegate.canCommit(tx, changeIds);
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) throws TransactionFailureException {
    delegate.canCommitOrThrow(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    //noinspection deprecation
    return delegate.commit(tx);
  }

  @Override
  public void commitOrThrow(Transaction tx) throws TransactionFailureException {
    delegate.commitOrThrow(tx);
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
  public void pruneNow() {
    delegate.pruneNow();
  }

  protected <V> V supplyWithRetries(Supplier<V> supplier) {
    return Retries.supplyWithRetries(supplier, retryStrategy, IS_RETRYABLE);
  }

  protected <V, T extends Throwable> V callWithRetries(Retries.Callable<V, T> callable) throws T {
    return Retries.callWithRetries(callable, retryStrategy, IS_RETRYABLE);
  }
}
