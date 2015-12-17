/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.DatasetProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Implementation of {@link DatasetProvider} that allows to dynamically load datasets
 * into a started {@link TransactionContext}. Datasets acquired from this context are distinct from any
 * Datasets instantiated outside this class.
 */
public class SingleThreadDatasetCache extends DynamicDatasetCache {

  private static final Logger LOG = LoggerFactory.getLogger(SingleThreadDatasetCache.class);

  private static final Iterable<TransactionAware> NO_TX_AWARES = ImmutableList.of();

  private final LoadingCache<DatasetCacheKey, Dataset> datasetCache;
  private final CacheLoader<DatasetCacheKey, Dataset> datasetLoader;
  private final Map<DatasetCacheKey, TransactionAware> activeTxAwares = new HashMap<>();
  private final Map<DatasetCacheKey, Dataset> staticDatasets = new HashMap<>();
  private final Set<TransactionAware> extraTxAwares = Sets.newIdentityHashSet();

  private DelayedDiscardingTransactionContext txContext = null;

  /**
   * See {@link DynamicDatasetCache}.
   *
   * @param staticDatasets  if non-null, a map from dataset name to runtime arguments. These datasets will be
   *                        instantiated immediately, and they will participate in every transaction started
   *                        through {@link #newTransactionContext()}.
   */
  public SingleThreadDatasetCache(final SystemDatasetInstantiator instantiator,
                                  final TransactionSystemClient txClient,
                                  final NamespaceId namespace,
                                  Map<String, String> runtimeArguments,
                                  @Nullable final MetricsContext metricsContext,
                                  @Nullable Map<String, Map<String, String>> staticDatasets) {
    super(instantiator, txClient, namespace, runtimeArguments, metricsContext);
    this.datasetLoader = new CacheLoader<DatasetCacheKey, Dataset>() {
      @Override
      @ParametersAreNonnullByDefault
      public Dataset load(DatasetCacheKey key) throws Exception {
        Dataset dataset = instantiator.getDataset(namespace.dataset(key.getName()), key.getArguments());
        if (dataset instanceof MeteredDataset && metricsContext != null) {
          ((MeteredDataset) dataset).setMetricsCollector(
            metricsContext.childContext(Constants.Metrics.Tag.DATASET, key.getName()));
        }
        return dataset;
      }
    };
    this.datasetCache = CacheBuilder.newBuilder().removalListener(
      new RemovalListener<DatasetCacheKey, Dataset>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<DatasetCacheKey, Dataset> notification) {
          closeDataset(notification.getKey(), notification.getValue());
        }
      })
      .build(datasetLoader);

    // add all the static datasets to the cache. This makes sure that a) the cache is preloaded and
    // b) if any static datasets cannot be loaded, the problem show right away (and not later). See
    // also the javadoc of this c'tor, which states that all static datasets get loaded right away.
    if (staticDatasets != null) {
      for (Map.Entry<String, Map<String, String>> entry : staticDatasets.entrySet()) {
        this.staticDatasets.put(new DatasetCacheKey(entry.getKey(), entry.getValue()),
                                getDataset(entry.getKey(), entry.getValue()));
      }
    }
  }

  private void closeDataset(DatasetCacheKey key, Dataset dataset) {
    // close the dataset
    if (dataset != null) {
      try {
        dataset.close();
      } catch (Throwable e) {
        LOG.warn(String.format("Error closing dataset '%s' of type %s",
                               String.valueOf(key), dataset.getClass().getName()), e);
      }
    }
  }

  @Override
  public <T extends Dataset> T getDataset(DatasetCacheKey key, boolean bypass)
    throws DatasetInstantiationException {

    Dataset dataset;
    try {
      if (bypass) {
        dataset = datasetLoader.load(key);
      } else {
        try {
          dataset = datasetCache.get(key);
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }
    } catch (Throwable t) {
      throw new DatasetInstantiationException(
        String.format("Could not instantiate dataset '%s'", key.getName()), t);
    }
    // make sure the dataset exists and is of the right type
    if (dataset == null) {
      throw new DatasetInstantiationException(String.format("Dataset '%s' does not exist", key.getName()));
    }
    T typedDataset;
    try {
      @SuppressWarnings("unchecked")
      T t = (T) dataset;
      typedDataset = t;
    } catch (Throwable t) { // must be ClassCastException
      throw new DatasetInstantiationException(
        String.format("Could not cast dataset '%s' to requested type. Actual type is %s.",
                      key.getName(), dataset.getClass().getName()), t);
    }

    // any transaction aware that is not in the active tx-awares is added to the current tx context (if there is one).
    if (!bypass && dataset instanceof TransactionAware) {
      TransactionAware txAware = (TransactionAware) dataset;
      TransactionAware existing = activeTxAwares.get(key);
      if (existing == null) {
        activeTxAwares.put(key, txAware);
        if (txContext != null) {
          txContext.addTransactionAware(txAware);
        }
      } else if (existing != dataset) {
        // this better be the same dataset, otherwise the cache did not work
        throw new IllegalStateException(
          String.format("Unexpected state: Cache returned %s for %s, which is different from the " +
                          "active transaction aware %s for the same key. This should never happen.",
                        dataset, key, existing));
      }
    }
    return typedDataset;
  }

  @Override
  public void discardDataset(Dataset dataset) {
    Preconditions.checkNotNull(dataset);
    // static datasets cannot be discarded
    if (staticDatasets.containsValue(dataset)) {
      LOG.warn("Attempt to discard static dataset {} from dataset cache", dataset);
      return;
    }
    if (txContext == null || !(dataset instanceof TransactionAware)) {
      discardSafely(dataset);
    } else {
      // it is a tx-aware: it may participate in a transaction, so mark it as to be discarded after the tx:
      // the transaction context will call discardSafely() for this dataset when the tx is complete.
      txContext.discardAfterTx((TransactionAware) dataset);
    }
    // remove from activeTxAwares in any case - a discarded dataset does not need to participate in external tx
    // iterates over all datasets but we do not expect this map to become large
    for (Map.Entry<DatasetCacheKey, TransactionAware> entry : activeTxAwares.entrySet()) {
      if (dataset == entry.getValue()) {
        activeTxAwares.remove(entry.getKey());
        return;
      }
    }
  }

  /**
   * Discard a dataset when it is known that no transaction is going on.
   *
   * @param dataset this is an Object because we need to pass in TransactionAware or Dataset
   */
  public void discardSafely(Object dataset) {
    // iterates over all datasets but we do not expect this map to become large
    for (Map.Entry<DatasetCacheKey, Dataset> entry : datasetCache.asMap().entrySet()) {
      if (dataset == entry.getValue()) {
        datasetCache.invalidate(entry.getKey());
        return;
      }
    }
    // we can only hope that dataset.toString() is meaningful
    LOG.warn("Attempt to discard a dataset that was not acquired through this provider: {}", dataset);
  }

  @Override
  public TransactionContext newTransactionContext() {
    dismissTransactionContext();
    txContext = new DelayedDiscardingTransactionContext(activeTxAwares.values(), extraTxAwares);
    return txContext;
  }

  @Override
  public void dismissTransactionContext() {
    if (txContext != null) {
      txContext.cleanup();
      txContext = null;
    }
  }

  @Override
  public Iterable<TransactionAware> getStaticTransactionAwares() {
    return Iterables.filter(staticDatasets.values(), TransactionAware.class);
  }

  @Override
  public Iterable<TransactionAware> getTransactionAwares() {
    if (txContext == null) {
      return NO_TX_AWARES;
    }
    return Iterables.concat(extraTxAwares, activeTxAwares.values());
  }

  @Override
  public void addExtraTransactionAware(TransactionAware txAware) {
    extraTxAwares.add(txAware);
    if (txContext != null) {
      txContext.addTransactionAware(txAware);
    }
  }

  @Override
  public void removeExtraTransactionAware(TransactionAware txAware) {
    extraTxAwares.remove(txAware);
    if (txContext != null) {
      txContext.removeTransactionAware(txAware);
    }
  }

  @Override
  public void invalidate() {
    dismissTransactionContext();
    activeTxAwares.clear();
    try {
      datasetCache.invalidateAll();
    } catch (Throwable t) {
      LOG.error("Error invalidating dataset cache", t);
    }
    try {
      datasetCache.cleanUp();
    } catch (Throwable t) {
      LOG.error("Error cleaning up dataset cache", t);
    }
  }

  @Override
  public void close() {
    for (TransactionAware txAware : extraTxAwares) {
      if (txAware instanceof Closeable) {
        Closeables.closeQuietly((Closeable) txAware);
      }
    }
    invalidate();
    super.close();
  }

  /**
   * This is an implementation of TransactionContext that delays the discarding of a transaction-aware
   * dataset until after the transaction is complete. This is needed in cases where a client calls
   * {@link DatasetProvider#discardDataset(Dataset)} in the middle of a transaction: The client indicates
   * that it does not need that dataset any more. But it is participating in the current transaction,
   * and needs to continue to do so until the transaction has ended. Therefore this class will put
   * that dataset on a toDiscard set, which is inspected after every transaction.
   */
  private class DelayedDiscardingTransactionContext extends TransactionContext {

    private final Collection<TransactionAware> txAwares;
    private final Collection<TransactionAware> toDiscard;
    private TransactionContext txContext;

    /**
     * Constructs the context from the transaction system client (needed by TransactionContext).
     */
    private DelayedDiscardingTransactionContext(Collection<TransactionAware> txAwares,
                                                Collection<TransactionAware> extraTxAwares) {
      super(txClient);
      this.toDiscard = Sets.newIdentityHashSet();
      this.txAwares = Sets.newIdentityHashSet();
      this.txAwares.addAll(txAwares);
      this.txAwares.addAll(extraTxAwares);
    }

    @Override
    public boolean addTransactionAware(TransactionAware txAware) {
      if (!txAwares.add(txAware)) {
        return false; // it must already be in the actual tx-context
      }
      // this is new, add it to current tx context
      if (txContext != null) {
        txContext.addTransactionAware(txAware);
      }
      // in case this was marked for discarding, remove that mark
      toDiscard.remove(txAware);
      return true;
    }

    @Override
    public boolean removeTransactionAware(TransactionAware txAware) {
      // if the actual tx-context is non-null, we are in the middle of a transaction, and can't remove the tx-aware
      // so just remove this from the tx-awares here, and the next transaction will be started without it.
      return txAwares.remove(txAware);
    }

    /**
     * Mark a tx-aware for discarding after the transaction is complete.
     */
    public void discardAfterTx(TransactionAware txAware) {
      toDiscard.add(txAware);
      txAwares.remove(txAware);
    }

    /**
     * Discards all datasets marked for discarding, through the dataset cache, and set the tx context to null.
     */
    public void cleanup() {
      for (TransactionAware txAware : toDiscard) {
        SingleThreadDatasetCache.this.discardSafely(txAware);
      }
      toDiscard.clear();
      txContext = null;
    }

    @Override
    public void start() throws TransactionFailureException {
      if (txContext != null && txContext.getCurrentTransaction() != null) {
        LOG.warn("Starting a new transaction while the previous transaction {} is still on-going. ",
                 txContext.getCurrentTransaction().getTransactionId());
        cleanup();
      }
      txContext = new TransactionContext(SingleThreadDatasetCache.this.txClient, txAwares);
      txContext.start();
    }

    @Override
    public void finish() throws TransactionFailureException {
      // copied from TransactionContext so it behaves exactly the same in this case
      Preconditions.checkState(txContext != null, "Cannot finish tx that has not been started");
      try {
        txContext.finish();
      } finally {
        cleanup();
      }
    }

    @Override
    public void checkpoint() throws TransactionFailureException {
      // copied from TransactionContext so it behaves exactly the same in this case
      Preconditions.checkState(txContext != null, "Cannot checkpoint tx that has not been started");
      txContext.checkpoint();
    }

    @Nullable
    @Override
    public Transaction getCurrentTransaction() {
      return txContext == null ? null : txContext.getCurrentTransaction();
    }

    @Override
    public void abort(TransactionFailureException cause) throws TransactionFailureException {
      if (txContext == null) {
        // same behavior as Tephra's TransactionContext
        // might be called by some generic exception handler even though already aborted/finished - we allow that
        return;
      }
      try {
        txContext.abort(cause);
      } finally {
        cleanup();
      }
    }
  }
}

