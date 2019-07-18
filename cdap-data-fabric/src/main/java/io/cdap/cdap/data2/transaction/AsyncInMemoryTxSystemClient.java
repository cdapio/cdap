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
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.inject.Inject;

/**
 * Transaction system client that uses asynchronous threads to
 * call transaction method calls. This helps protect the the
 * (in-memory) transaction manager from being interrupted while
 * writing to the WAL or transaction state.
 */
public class AsyncInMemoryTxSystemClient implements TransactionSystemClient {

  private final TransactionSystemClient delegate;
  private final Executor executor;

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @Inject
  public AsyncInMemoryTxSystemClient(InMemoryTxSystemClient delegate) {
    this.delegate = delegate;
    this.executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("async-tx-client-%d").build());
  }

  private <T, E extends Exception> T call(Callable<T> callable, Class<? extends E> exceptionClass) throws E {
    SettableFuture<T> result = SettableFuture.create();
    this.executor.execute(() -> {
      try {
        result.set(callable.call());
      } catch (Throwable t) {
        result.setException(t);
      }
    });
    try {
      return Uninterruptibles.getUninterruptibly(result);
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), exceptionClass);
      throw Throwables.propagate(e.getCause());
    }
  }

  private <T> T call(Callable<T> callable) {
    return call(callable, RuntimeException.class);
  }

  private <E extends Exception> void run(ThrowingRunnable runnable, Class<? extends E> exceptionClass) throws E {
    call(() -> {
      runnable.run();
      return null;
    }, exceptionClass);
  }

  private void run(Runnable runnable) {
    run(runnable::run, RuntimeException.class);
  }

  @Override
  public Transaction startShort() {
    return call(delegate::startShort);
  }

  @Override
  public Transaction startShort(int timeout) {
    return call(() -> delegate.startShort(timeout));
  }

  @Override
  public Transaction startLong() {
    return call(delegate::startLong);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return call(() -> delegate.canCommit(tx, changeIds), TransactionNotInProgressException.class);
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) throws TransactionFailureException {
    run(() -> delegate.canCommitOrThrow(tx, changeIds), TransactionFailureException.class);
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    return call(() -> delegate.commit(tx), TransactionNotInProgressException.class);
  }

  @Override
  public void commitOrThrow(Transaction tx) throws TransactionFailureException {
    run(() -> delegate.commitOrThrow(tx), TransactionFailureException.class);
  }

  @Override
  public void abort(Transaction tx) {
    run(() -> delegate.abort(tx));
  }

  @Override
  public boolean invalidate(long tx) {
    return call(() -> delegate.invalidate(tx));
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    return call(() -> delegate.checkpoint(tx), TransactionNotInProgressException.class);
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    return call(delegate::getSnapshotInputStream, TransactionCouldNotTakeSnapshotException.class);
  }

  @Override
  public String status() {
    return call(delegate::status);
  }

  @Override
  public void resetState() {
    run(delegate::resetState);
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return call(() -> delegate.truncateInvalidTx(invalidTxIds));
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return call(() -> delegate.truncateInvalidTxBefore(time), InvalidTruncateTimeException.class);
  }

  @Override
  public int getInvalidSize() {
    return call(delegate::getInvalidSize);
  }

  @Override
  public void pruneNow() {
    run(delegate::pruneNow);
  }
}
