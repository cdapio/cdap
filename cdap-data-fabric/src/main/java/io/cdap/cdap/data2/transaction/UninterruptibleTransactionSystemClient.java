/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.transaction;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A {@link TransactionSystemClient} that executes calls that can't be interrupted by {@link Thread#interrupt()}.
 */
@SuppressWarnings("deprecation")
public final class UninterruptibleTransactionSystemClient implements TransactionSystemClient {

  private final ExecutorService executor;
  private final TransactionSystemClient delegate;

  public UninterruptibleTransactionSystemClient(TransactionSystemClient delegate) {
    this.delegate = delegate;
    this.executor = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("unint-tx-client-%d"));
  }

  @Override
  public Transaction startShort() {
    return execute((Callable<Transaction>) delegate::startShort);
  }

  @Override
  public Transaction startShort(int timeout) {
    return execute(() -> delegate.startShort(timeout));
  }

  @Override
  public Transaction startLong() {
    return execute(delegate::startLong);
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return execute(() -> delegate.canCommit(tx, changeIds), TransactionNotInProgressException.class);
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) throws TransactionFailureException {
    execute(() -> {
      delegate.canCommitOrThrow(tx, changeIds);
      return null;
    }, TransactionFailureException.class);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    return execute(() -> delegate.commit(tx), TransactionNotInProgressException.class);
  }

  @Override
  public void commitOrThrow(Transaction tx) throws TransactionFailureException {
    execute(() -> {
      delegate.commitOrThrow(tx);
      return null;
    }, TransactionFailureException.class);
  }

  @Override
  public void abort(Transaction tx) {
    execute(() -> delegate.abort(tx));
  }

  @Override
  public boolean invalidate(long tx) {
    return execute(() -> delegate.invalidate(tx));
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    return execute(() -> delegate.checkpoint(tx), TransactionNotInProgressException.class);
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    return execute(delegate::getSnapshotInputStream, TransactionCouldNotTakeSnapshotException.class);
  }

  @Override
  public String status() {
    return execute(delegate::status);
  }

  @Override
  public void resetState() {
    execute(delegate::resetState);
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return execute(() -> delegate.truncateInvalidTx(invalidTxIds));
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return execute(() -> delegate.truncateInvalidTxBefore(time), InvalidTruncateTimeException.class);
  }

  @Override
  public int getInvalidSize() {
    return execute(delegate::getInvalidSize);
  }

  @Override
  public void pruneNow() {
    execute(delegate::pruneNow);
  }

  private void execute(Runnable runnable) {
    execute(() -> {
      runnable.run();
      return null;
    });
  }

  private <V> V execute(Callable<V> callable) {
    return execute(callable, RuntimeException.class);
  }

  private <V, E extends Exception> V execute(Callable<V> callable, Class<? extends E> exClass) throws E {
    try {
      return Uninterruptibles.getUninterruptibly(executor.submit(callable));
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), exClass);
      throw new RuntimeException(e.getCause());
    }
  }
}
