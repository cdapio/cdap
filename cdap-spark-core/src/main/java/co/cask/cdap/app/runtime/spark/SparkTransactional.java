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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link Transactional} for managing transactions for Spark job execution.
 */
final class SparkTransactional implements Transactional {

  /**
   * Property key for storing the key to lookup active transaction
   */
  static final String ACTIVE_TRANSACTION_KEY = "cdap.spark.active.transaction";

  private static final Logger LOG = LoggerFactory.getLogger(SparkTransactional.class);

  private final ThreadLocal<TransactionalDatasetContext> activeDatasetContext =
    new ThreadLocal<TransactionalDatasetContext>() {
      @Override
      public void set(TransactionalDatasetContext value) {
        String txKey = Long.toString(value.getTransaction().getWritePointer());
        if (SparkContextCache.setLocalProperty(ACTIVE_TRANSACTION_KEY, txKey)) {
          transactionInfos.put(txKey, value);
        }

        super.set(value);
      }

      @Override
      public void remove() {
        String txKey = SparkContextCache.getLocalProperty(ACTIVE_TRANSACTION_KEY);
        if (txKey != null && !txKey.isEmpty()) {
          // Spark doesn't support unsetting of property. Hence set it to empty.
          SparkContextCache.setLocalProperty(ACTIVE_TRANSACTION_KEY, "");
          transactionInfos.remove(txKey);
        }
        super.remove();
      }
  };

  private final TransactionSystemClient txClient;
  private final DynamicDatasetCache datasetCache;
  private final Map<String, TransactionalDatasetContext> transactionInfos;

  SparkTransactional(TransactionSystemClient txClient, DynamicDatasetCache datasetCache) {
    this.txClient = txClient;
    this.datasetCache = datasetCache;
    this.transactionInfos = new ConcurrentHashMap<>();
  }

  /**
   * Executes the given {@link TxRunnable} with a long {@link Transaction}. All Spark RDD operations performed
   * inside the given {@link TxRunnable} will be using the same {@link Transaction}.
   *
   * @param runnable the runnable to be executed in the transaction
   * @throws TransactionFailureException if there is failure during execution. The actual cause of the failure
   *                                     maybe wrapped inside the {@link TransactionFailureException} (both
   *                                     user exception from the {@link TxRunnable#run(DatasetContext)} method
   *                                     or transaction exception from Tephra).
   */
  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    executeLong(wrap(runnable), false);
  }

  @Nullable
  TransactionInfo getTransactionInfo(String key) {
    return transactionInfos.get(key);
  }

  /**
   * Attempts to execute the given {@link TxRunnable} with an active transaction first. If there is no active
   * transaction, it will be executed with a new long {@link Transaction}.
   *
   * @param runnable The {@link TxRunnable} to be executed inside a transaction
   * @param asyncCommit Only used if a new long {@link Transaction} is created.
   *                    If {@code true}, the transaction will be left open and it is expected to be committed
   *                    by external entity asynchronously after this method returns;
   *                    if {@code false}, the transaction will be committed when the runnable returned.
   * @throws TransactionFailureException
   */
  void executeWithActiveOrLongTx(SparkTxRunnable runnable, boolean asyncCommit) throws TransactionFailureException {
    if (!tryExecuteWithActiveTransaction(runnable)) {
      executeLong(runnable, asyncCommit);
    }
  }

  /**
   * Attempts to execute the given {@link TxRunnable} with an active transaction. If there is no active transaction,
   * the {@link TxRunnable#run(DatasetContext)} will not be called.
   *
   * @param runnable the {@link TxRunnable} to run
   * @return {@code true} if there is an active transaction and the {@link TxRunnable} was executed
   * @throws TransactionFailureException if execute of the {@link TxRunnable} failed.
   */
  private boolean tryExecuteWithActiveTransaction(SparkTxRunnable runnable) throws TransactionFailureException {
    try {
      TransactionalDatasetContext datasetContext = activeDatasetContext.get();
      if (datasetContext == null || !datasetContext.canUse()) {
        return false;
      }

      // Call the runnable
      runnable.run(datasetContext);

      // Persist all changes.
      datasetContext.flush();

      // Successfully execute with the current active transaction
      return true;
    } catch (Throwable t) {
      throw Transactions.asTransactionFailure(t);
    }
  }

  /**
   * Executes the given runnable with a new long transaction. It will check for nested transaction and throws a
   * {@link TransactionFailureException} if it finds one.
   *
   * @param runnable The {@link TxRunnable} to be executed inside a transaction
   * @param asyncCommit if {@code true}, the transaction will be left open and it is expected to be committed
   *                    by external entity asynchronously after this method returns;
   *                    if {@code false}, the transaction will be committed when the runnable returned.
   *
   */
  private void executeLong(SparkTxRunnable runnable, boolean asyncCommit) throws TransactionFailureException {
    TransactionalDatasetContext transactionalDatasetContext = activeDatasetContext.get();
    if (transactionalDatasetContext != null) {
      try {
        if (!transactionalDatasetContext.commitOnJobEnded()) {
          throw new TransactionFailureException("Nested transaction not supported. Active transaction is "
                                                  + transactionalDatasetContext.getTransaction());
        }

        if (transactionalDatasetContext.isJobStarted()) {
          // Wait for the completion of the active transaction. This is needed because the commit is done
          // asynchronously through the SparkListener.
          transactionalDatasetContext.awaitCompletion();
        } else if (!asyncCommit) {
          // For implicit transaction (commitOnJobEnd == true), if the job hasn't been started,
          // we can use that transaction.
          // If asyncCommit is false, change the transaction to be committed at the end of this method instead
          // of on job end. This is for the case when there is an opening of implicit transaction (e.g. reduceByKey),
          // followed by an explicit transaction.
          transactionalDatasetContext.useSyncCommit();
          LOG.debug("Converted implicit transaction to explicit transaction: {}",
                    transactionalDatasetContext.getTransaction().getWritePointer());
        }
      } catch (InterruptedException e) {
        // Don't execute the runnable. Reset the interrupt flag and return
        Thread.currentThread().interrupt();
        return;
      }
    }

    // Start a new long transaction
    Transaction transaction = txClient.startLong();
    try {
      // Create a DatasetContext that uses the newly created transaction
      TransactionalDatasetContext datasetContext = new TransactionalDatasetContext(transaction, datasetCache,
                                                                                   asyncCommit);
      activeDatasetContext.set(datasetContext);

      // Call the runnable
      runnable.run(datasetContext);

      // Persist the changes
      datasetContext.flush();

      if (!asyncCommit) {
        // Commit transaction
        if (!txClient.commit(datasetContext.getTransaction())) {
          throw new TransactionFailureException("Failed to commit explicit transaction " + transaction);
        }
        activeDatasetContext.remove();
        datasetContext.postCommit();
        datasetContext.discardDatasets();
      }
    } catch (Throwable t) {
      // Any exception will cause invalidation of the transaction
      activeDatasetContext.remove();
      Transactions.invalidateQuietly(txClient, transaction);
      throw Transactions.asTransactionFailure(t);
    }
  }

  private SparkTxRunnable wrap(final TxRunnable runnable) {
    return new SparkTxRunnable() {
      @Override
      public void run(SparkDatasetContext context) throws Exception {
        runnable.run(context);
      }
    };
  }

  /**
   * A {@link DatasetContext} to be used for the transactional execution. All {@link Dataset} instance
   * created through instance of this class will be using the same transaction.
   *
   * Instance of this class is safe to use from multiple threads concurrently. This is for supporting Spark program
   * with multiple threads that drive computation concurrently within the same transaction.
   */
  @ThreadSafe
  private final class TransactionalDatasetContext implements SparkDatasetContext, TransactionInfo {

    private final Transaction transaction;
    private final DynamicDatasetCache datasetCache;
    private final Set<Dataset> datasets;
    private final Set<Dataset> discardDatasets;
    private CountDownLatch completion;
    private volatile boolean jobStarted;

    private TransactionalDatasetContext(Transaction transaction,
                                        DynamicDatasetCache datasetCache, boolean asyncCommit) {
      this.transaction = transaction;
      this.datasetCache = datasetCache;
      this.datasets = Collections.synchronizedSet(new HashSet<Dataset>());
      this.discardDatasets = Collections.synchronizedSet(new HashSet<Dataset>());
      this.completion = asyncCommit ? new CountDownLatch(1) : null;
    }

    void useSyncCommit() {
      // This shouldn't happen as the executeLong will check for isJobStarted before calling this method
      Preconditions.checkState(commitOnJobEnded() && !isJobStarted(),
                               "Job execution already started, cannot change commit method to sync commit.");
      this.completion = null;
    }

    boolean isJobStarted() {
      return jobStarted;
    }

    @Override
    @Nonnull
    public Transaction getTransaction() {
      return transaction;
    }

    @Override
    public boolean commitOnJobEnded() {
      return completion != null;
    }

    @Override
    public void onJobStarted() {
      jobStarted = true;
    }

    @Override
    public void onTransactionCompleted(boolean jobSucceeded, @Nullable TransactionFailureException failureCause) {
      // Shouldn't happen
      Preconditions.checkState(commitOnJobEnded(), "Not expecting transaction to be completed");
      transactionInfos.remove(Long.toString(transaction.getWritePointer()));
      if (failureCause == null) {
        postCommit();
      }
      completion.countDown();
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return getDataset(name, Collections.<String, String>emptyMap());
    }

    @Override
    public <T extends Dataset> T getDataset(String name,
                                            Map<String, String> arguments) throws DatasetInstantiationException {
      return getDataset(name, arguments, AccessType.UNKNOWN);
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments,
                                            AccessType accessType) throws DatasetInstantiationException {
      T dataset = datasetCache.getDataset(name, arguments, accessType);

      // Only call startTx if the dataset hasn't been seen before
      // It is ok because there is only one transaction in this DatasetContext
      // If a dataset instance is being reused, we don't need to call startTx again.
      // It's also true for the case when a dataset instance was released and reused.
      if (datasets.add(dataset) && dataset instanceof TransactionAware) {
        ((TransactionAware) dataset).startTx(transaction);
      }

      return dataset;
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      discardDataset(dataset);
    }

    @Override
    public void discardDataset(Dataset dataset) {
      discardDatasets.add(dataset);
    }

    /**
     * Flushes all {@link TransactionAware} that were acquired through this {@link DatasetContext} by calling
     * {@link TransactionAware#commitTx()}.
     *
     * @throws TransactionFailureException if any {@link TransactionAware#commitTx()} call throws exception
     */
    private void flush() throws TransactionFailureException {
      for (TransactionAware txAware : Iterables.filter(datasets, TransactionAware.class)) {
        try {
          if (!txAware.commitTx()) {
            throw new TransactionFailureException("Failed to persist changes for " + txAware);
          }
        } catch (Throwable t) {
          throw Transactions.asTransactionFailure(t);
        }
      }
    }

    /**
     * Calls {@link TransactionAware#postTxCommit()} methods on all {@link TransactionAware} acquired through this
     * {@link DatasetContext}.
     */
    private void postCommit() {
      for (TransactionAware txAware : Iterables.filter(datasets, TransactionAware.class)) {
        try {
          txAware.postTxCommit();
        } catch (Exception e) {
          LOG.warn("Exception raised in postTxCommit call on dataset {}", txAware, e);
        }
      }
    }

    /**
     * Discards all datasets that has {@link #discardDataset(Dataset)} called before
     */
    private void discardDatasets() {
      for (Dataset dataset : discardDatasets) {
        datasetCache.discardDataset(dataset);
      }
      discardDatasets.clear();
      datasets.clear();
    }

    /**
     * Block until the transaction used by this context is completed (either commit or invalidate).
     *
     * @throws InterruptedException if current thread is interrupted while waiting
     */
    private void awaitCompletion() throws InterruptedException {
      LOG.debug("Awaiting completion for {}", transaction.getWritePointer());
      if (completion != null) {
        completion.await();
      }
      discardDatasets();
    }

    /**
     * Returns {@code true} if this context can be used for executing {@link TxRunnable}.
     */
    private boolean canUse() {
      // If the commit is done when a Spark job end and the job already started, then this context cannot be used
      // anymore. This is to guard against case like this:
      //
      // sc.fromDataset("dataset1")
      //   .map(...)
      //   .reduce(...)   <-- This will start a transaction (tx1) that will commit on job end (due to fromDataset)
      //                      it will also start a spark job.
      //
      // sc.fromDataset("dataset2")
      //   .join(sc.fromDataset("dataset3"))  <-- This needs a transaction to get partitions from "dataset2"
      //                                          and "dataset3". However, we will get "tx1" from the thread local,
      //                                          which we shouldn't use for this job. In fact, we need to block on
      //                                          completion on "tx1", which is done asynchronously by the
      //                                          SparkListener callback, until we start another transaction in this
      //                                          thread.
      return !commitOnJobEnded() || !jobStarted;
    }
  }
}
