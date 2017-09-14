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
import com.google.common.base.Supplier;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collection;

/**
 * Retries methods used during long transactions.
 */
public class RetryingLongTransactionSystemClient extends RetryingTransactionSystemClient {

  public RetryingLongTransactionSystemClient(TransactionSystemClient delegate, RetryStrategy retryStrategy) {
    super(delegate, retryStrategy);
  }

  @Override
  public Transaction startLong() {
    return supplyWithRetries(new Supplier<Transaction>() {
      @Override
      public Transaction get() {
        return delegate.startLong();
      }
    });
  }

  @Override
  public boolean canCommit(final Transaction tx,
                           final Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return callWithRetries(new Retries.Callable<Boolean, TransactionNotInProgressException>() {
      @Override
      public Boolean call() throws TransactionNotInProgressException {
        //noinspection deprecation
        return delegate.canCommit(tx, changeIds);
      }
    });
  }

  @Override
  public void canCommitOrThrow(final Transaction tx,
                               final Collection<byte[]> changeIds) throws TransactionFailureException {
    callWithRetries(new Retries.Callable<Void, TransactionFailureException>() {
      @Override
      public Void call() throws TransactionFailureException {
        delegate.canCommitOrThrow(tx, changeIds);
        return null;
      }
    });
  }

  @Override
  public boolean commit(final Transaction tx) throws TransactionNotInProgressException {
    return callWithRetries(new Retries.Callable<Boolean, TransactionNotInProgressException>() {
      @Override
      public Boolean call() throws TransactionNotInProgressException {
        //noinspection deprecation
        return delegate.commit(tx);
      }
    });
  }

  @Override
  public void commitOrThrow(final Transaction tx) throws TransactionFailureException {
    callWithRetries(new Retries.Callable<Void, TransactionFailureException>() {
      @Override
      public Void call() throws TransactionFailureException {
        delegate.commitOrThrow(tx);
        return null;
      }
    });
  }

  @Override
  public void abort(final Transaction tx) {
    supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        delegate.abort(tx);
        return null;
      }
    });
  }

  @Override
  public boolean invalidate(final long tx) {
    return supplyWithRetries(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return delegate.invalidate(tx);
      }
    });
  }
}
