/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.transaction.AbstractTransactionContext;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.ForwardingLoadingCache;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Implementation of {@link DatasetContext} that allows to dynamically load datasets
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
  private final Deque<TransactionAware> extraTxAwares = new LinkedList<>();

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
    super(instantiator, txClient, namespace, runtimeArguments);
    this.datasetLoader = new CacheLoader<DatasetCacheKey, Dataset>() {
      @Override
      @ParametersAreNonnullByDefault
      public Dataset load(DatasetCacheKey key) throws Exception {
        Dataset dataset = instantiator.getDataset(new DatasetId(key.getNamespace(), key.getName()),
                                                  // avoid DatasetDefinition or Dataset from modifying the cache key
                                                  ImmutableMap.copyOf(key.getArguments()), key.getAccessType());
        if (dataset instanceof MeteredDataset && metricsContext != null) {
          ((MeteredDataset) dataset).setMetricsCollector(
            metricsContext.childContext(Constants.Metrics.Tag.DATASET, key.getName()));
        }
        return dataset;
      }
    };
    LoadingCache<DatasetCacheKey, Dataset> delegate = CacheBuilder.newBuilder().removalListener(
      new RemovalListener<DatasetCacheKey, Dataset>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<DatasetCacheKey, Dataset> notification) {
          closeDataset(notification.getKey(), notification.getValue());
        }
      })
      .build(datasetLoader);

    this.datasetCache = new LineageRecordingDatasetCache(delegate, instantiator, namespace);

    // add all the static datasets to the cache. This makes sure that a) the cache is preloaded and
    // b) if any static datasets cannot be loaded, the problem show right away (and not later). See
    // also the javadoc of this c'tor, which states that all static datasets get loaded right away.
    if (staticDatasets != null) {
      for (Map.Entry<String, Map<String, String>> entry : staticDatasets.entrySet()) {
        this.staticDatasets.put(new DatasetCacheKey(namespace.getNamespace(), entry.getKey(), entry.getValue()),
                                getDataset(entry.getKey(), entry.getValue()));
      }
    }
  }

  /**
   * Cache that records lineage for a dataset access each time the dataset is requested.
   */
  private static final class LineageRecordingDatasetCache
    extends ForwardingLoadingCache.SimpleForwardingLoadingCache<DatasetCacheKey, Dataset> {

    private final SystemDatasetInstantiator instantiator;
    private final NamespaceId namespaceId;

    private LineageRecordingDatasetCache(LoadingCache<DatasetCacheKey, Dataset> delegate,
                                         SystemDatasetInstantiator instantiator, NamespaceId namespaceId) {
      super(delegate);
      this.instantiator = instantiator;
      this.namespaceId = namespaceId;
    }

    @Override
    public Dataset get(DatasetCacheKey key) throws ExecutionException {
      // write lineage information on each get call
      instantiator.writeLineage(namespaceId.dataset(key.getName()), key.getAccessType());
      return super.get(key);
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
        } catch (ExecutionException | UncheckedExecutionException e) {
          throw e.getCause();
        }
      }
    } catch (DatasetInstantiationException | ServiceUnavailableException e) {
      throw e;
    } catch (Throwable t) {
      throw new DatasetInstantiationException(
        String.format("Could not instantiate dataset '%s:%s'", key.getNamespace(), key.getName()), t);
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
  private void discardSafely(Object dataset) {
    // iterates over all datasets but we do not expect this map to become large
    for (Map.Entry<DatasetCacheKey, Dataset> entry : datasetCache.asMap().entrySet()) {
      if (dataset == entry.getValue()) {
        datasetCache.invalidate(entry.getKey());
        return;
      }
    }
    // we can only hope that dataset.toString() is meaningful
    LOG.warn("Attempt to discard a dataset that was not acquired through this context: {}", dataset);
  }

  @Override
  public TransactionContext newTransactionContext() throws TransactionFailureException {
    if (txContext != null && txContext.getCurrentTransaction() != null) {
      throw new TransactionFailureException("Attempted to start a transaction within active transaction " +
                                              txContext.getCurrentTransaction().getTransactionId());
    }
    dismissTransactionContext();
    txContext = new DelayedDiscardingTransactionContext(txClient, activeTxAwares.values());
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
    return (txContext == null) ? NO_TX_AWARES : Iterables.concat(activeTxAwares.values(), extraTxAwares);
  }

  @Override
  public Iterable<TransactionAware> getExtraTransactionAwares() {
    return extraTxAwares;
  }

  @Override
  public void addExtraTransactionAware(TransactionAware txAware) {
    // Use LIFO ordering. This method is only used for internal TransactionAware
    // like Flow Queue publisher/consumer and TMS publisher/fetcher
    // Using LIFO ordering is to allow TMS tx aware always be the last.
    if (!extraTxAwares.contains(txAware)) {
      extraTxAwares.addFirst(txAware);

      // Starts the transaction on the tx aware if there is an active transaction
      Transaction currentTx = txContext == null ? null : txContext.getCurrentTransaction();
      if (currentTx != null) {
        txAware.startTx(currentTx);
      }
    }
  }

  @Override
  public void removeExtraTransactionAware(TransactionAware txAware) {
    if (extraTxAwares.contains(txAware)) {
      Preconditions.checkState(txContext == null || txContext.getCurrentTransaction() == null,
                               "Cannot remove TransactionAware while there is an active transaction.");
      extraTxAwares.remove(txAware);
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
   * {@link DatasetContext#discardDataset(Dataset)} in the middle of a transaction: The client indicates
   * that it does not need that dataset any more. But it is participating in the current transaction,
   * and needs to continue to do so until the transaction has ended. Therefore this class will put
   * that dataset on a toDiscard set, which is inspected after every transaction.
   */
  private final class DelayedDiscardingTransactionContext extends AbstractTransactionContext {

    private final Set<TransactionAware> regularTxAwares;
    private final Set<TransactionAware> toDiscard;
    private final Iterable<TransactionAware> allTxAwares;

    DelayedDiscardingTransactionContext(TransactionSystemClient txClient, Iterable<TransactionAware> txAwares) {
      super(txClient);
      this.regularTxAwares = Sets.newIdentityHashSet();
      this.toDiscard = Sets.newIdentityHashSet();

      Iterables.addAll(regularTxAwares, txAwares);
      this.allTxAwares = Iterables.concat(regularTxAwares, toDiscard, extraTxAwares);
    }

    @Override
    protected Iterable<TransactionAware> getTransactionAwares() {
      return allTxAwares;
    }

    @Override
    protected boolean doAddTransactionAware(TransactionAware txAware) {
      if (!regularTxAwares.add(txAware)) {
        return false;
      }
      // In case this was marked for discarding, remove that mark.
      // If the txAware is in the discard set, treat it as non-new tx awares, hence it won't try to start a new TX on it
      return !toDiscard.remove(txAware);
    }

    @Override
    protected boolean doRemoveTransactionAware(TransactionAware txAware) {
      // This method only gets called when there is no active transaction,
      // hence no need to memorize the tx aware in the discard set
      return regularTxAwares.remove(txAware);
    }

    /**
     * Discards all datasets marked for discarding, through the dataset cache, and set the tx context to null.
     */
    @Override
    protected void cleanup() {
      for (TransactionAware txAware : toDiscard) {
        SingleThreadDatasetCache.this.discardSafely(txAware);
      }
      toDiscard.clear();
    }

    /**
     * Mark a tx-aware for discarding after the transaction is complete.
     */
    private void discardAfterTx(TransactionAware txAware) {
      if (regularTxAwares.remove(txAware)) {
        toDiscard.add(txAware);
      }
    }
  }
}

