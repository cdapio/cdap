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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

import java.util.Collection;

/**
 * An abstract base class for implementing {@link TransactionAware} that is thread aware.
 *
 * @param <T> type of {@link TransactionAware} used in each thread.
 */
public abstract class MultiThreadTransactionAware<T extends TransactionAware> implements TransactionAware {

  private final LoadingCache<Thread, T> perThreadTransactionAwares =
    CacheBuilder.newBuilder().weakKeys()
      .removalListener(new RemovalListener<Thread, T>() {
        @Override
        public void onRemoval(RemovalNotification<Thread, T> notification) {
          T value = notification.getValue();
          if (value != null) {
            MultiThreadTransactionAware.this.onRemoval(value);
          }
        }
      })
      .build(new CacheLoader<Thread, T>() {
        @Override
        public T load(Thread thread) throws Exception {
          return createTransactionAwareForCurrentThread();
        }
      });

  protected abstract T createTransactionAwareForCurrentThread();

  protected void onRemoval(T txAware) {
    // no-op
  }

  protected final T getCurrentThreadTransactionAware() {
    return perThreadTransactionAwares.getUnchecked(Thread.currentThread());
  }

  @Override
  public final void startTx(Transaction transaction) {
    getCurrentThreadTransactionAware().startTx(transaction);
  }

  @Override
  public final void updateTx(Transaction transaction) {
    getCurrentThreadTransactionAware().updateTx(transaction);
  }

  @Override
  public final Collection<byte[]> getTxChanges() {
    return getCurrentThreadTransactionAware().getTxChanges();
  }

  @Override
  public final boolean commitTx() throws Exception {
    return getCurrentThreadTransactionAware().commitTx();
  }

  @Override
  public final void postTxCommit() {
    getCurrentThreadTransactionAware().postTxCommit();
  }

  @Override
  public final boolean rollbackTx() throws Exception {
    return getCurrentThreadTransactionAware().rollbackTx();
  }

  @Override
  public final String getTransactionAwareName() {
    return getCurrentThreadTransactionAware().getTransactionAwareName();
  }
}
