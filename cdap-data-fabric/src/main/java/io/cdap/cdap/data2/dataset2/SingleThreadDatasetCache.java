/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.metrics.MeteredDataset;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.transaction.AbstractTransactionContext;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Implementation of {@link DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}. Datasets acquired from this context are distinct from any
 * Datasets instantiated outside this class.
 */
public class SingleThreadDatasetCache extends DynamicDatasetCache {

  private static final Logger LOG = LoggerFactory.getLogger(SingleThreadDatasetCache.class);

  private static final Iterable<TransactionAware> NO_TX_AWARES = Collections.emptyList();

  private final LoadingCache<AccessAwareDatasetCacheKey, Dataset> datasetCache;
  private final Map<DatasetCacheKey, TransactionAware> activeTxAwares = new HashMap<>();
  private final Map<DatasetCacheKey, Dataset> staticDatasets = new HashMap<>();
  private final Deque<TransactionAware> extraTxAwares = new LinkedList<>();
  private final MetricsContext metricsContext;

  private DelayedDiscardingTransactionContext txContext;

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
    this.metricsContext = metricsContext;

    // This is a dataset cache for the actual dataset instance. Different access type of the same dataset id will
    // give the same instance.
    LoadingCache<DatasetCacheKey, Dataset> datasetInstanceCache = CacheBuilder.newBuilder().removalListener(
      new RemovalListener<DatasetCacheKey, Dataset>() {
        @ParametersAreNonnullByDefault
        @Override
        public void onRemoval(RemovalNotification<DatasetCacheKey, Dataset> notification) {
          closeDataset(notification.getKey(), notification.getValue());
        }
      }).build(new CacheLoader<DatasetCacheKey, Dataset>() {
        @ParametersAreNonnullByDefault
        @Override
        public Dataset load(DatasetCacheKey key) throws Exception {
          return createDatasetInstance(key, false);
        }
      });


    // This is the cache used by this class and the cache key is access type aware.
    // It gets the dataset instance from the datasetInstanceCache above.
    // Calling this cache with the same dataset id but different access type will return the same dataset instance,
    // although there will be multiple entries created in this cache, which we rely on to track
    // lineage of different types.
    // On entry invalidation, we either invalidate all cached entries that has the same dataset instance as the value,
    // or we perform invalidateAll, hence we don't need to worry about closing dataset on one entry, while having
    // another entry that pointing to the same dataset instance.
    this.datasetCache = CacheBuilder.newBuilder().removalListener(
      new RemovalListener<AccessAwareDatasetCacheKey, Dataset>() {
        @ParametersAreNonnullByDefault
        @Override
        public void onRemoval(RemovalNotification<AccessAwareDatasetCacheKey, Dataset> notification) {
          if (notification.getKey() != null) {
            // Invalidate the dataset instance cache, which will result in closing the dataset.
            datasetInstanceCache.invalidate(notification.getKey().getDatasetCacheKey());
          }
        }
      }).build(new CacheLoader<AccessAwareDatasetCacheKey, Dataset>() {
        @ParametersAreNonnullByDefault
        @Override
        public Dataset load(AccessAwareDatasetCacheKey key) throws Exception {
          DatasetCacheKey cacheKey = key.getDatasetCacheKey();
          Dataset instance = datasetInstanceCache.get(cacheKey);
          instantiator.writeLineage(new DatasetId(cacheKey.getNamespace(), cacheKey.getName()),
                                    cacheKey.getAccessType());
          return instance;
        }
      });

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
        dataset = createDatasetInstance(key, true);
      } else {
        try {
          dataset = datasetCache.get(new AccessAwareDatasetCacheKey(key));
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
    Set<AccessType> accessTypes = EnumSet.allOf(AccessType.class);
    for (Map.Entry<AccessAwareDatasetCacheKey, Dataset> entry : datasetCache.asMap().entrySet()) {
      if (dataset == entry.getValue()) {
        datasetCache.invalidate(entry.getKey());
        // Make sure all unique access type are removed from the cache before breaking
        // This will ensure all different cache entries that shares the same underlying dataset instance gets
        // invalidated
        accessTypes.remove(entry.getKey().getDatasetCacheKey().getAccessType());
        if (accessTypes.isEmpty()) {
          return;
        }
      }
    }

    // CDAP-14326 check if at least one of the entries of the given dataset was invalidated from datasetCache. This
    // means dataset provided was acquired through this context. Hence return without logging any warning.
    if (accessTypes.size() < AccessType.values().length) {
      return;
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
    return staticDatasets.values().stream()
      .filter(TransactionAware.class::isInstance)
      .map(TransactionAware.class::cast)::iterator;
  }

  @Override
  public Iterable<TransactionAware> getTransactionAwares() {
    return (txContext == null)
      ? NO_TX_AWARES
      : Stream.concat(activeTxAwares.values().stream(), extraTxAwares.stream())::iterator;
  }

  @Override
  public Iterable<TransactionAware> getExtraTransactionAwares() {
    return extraTxAwares;
  }

  @Override
  public void addExtraTransactionAware(TransactionAware txAware) {
    // Use LIFO ordering. This method is only used for internal TransactionAware
    // like TMS publisher/fetcher
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
   * Creates a new instance of a dataset based on the given information.
   */
  private Dataset createDatasetInstance(DatasetCacheKey key, boolean recordLineage) {
    DatasetId datasetId = new DatasetId(key.getNamespace(), key.getName());
    Dataset dataset = instantiator.getDataset(datasetId, key.getArguments(), key.getAccessType());
    if (dataset instanceof MeteredDataset && metricsContext != null) {
      ((MeteredDataset) dataset).setMetricsCollector(
        metricsContext.childContext(Constants.Metrics.Tag.DATASET, key.getName()));
    }

    if (recordLineage) {
      instantiator.writeLineage(datasetId, key.getAccessType());
    }
    return dataset;
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

  /**
   * Cache key the wrap around {@link DatasetCacheKey}. Unlike the {@link DatasetCacheKey}, the comparison includes the
   * {@link DatasetCacheKey#accessType}.
   */
  private static final class AccessAwareDatasetCacheKey {

    private final DatasetCacheKey key;

    private AccessAwareDatasetCacheKey(DatasetCacheKey key) {
      this.key = key;
    }

    DatasetCacheKey getDatasetCacheKey() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AccessAwareDatasetCacheKey other = (AccessAwareDatasetCacheKey) o;
      return Objects.equals(key, other.key) && Objects.equals(key.getAccessType(), other.key.getAccessType());
    }

    @Override
    public int hashCode() {
      return 31 * key.hashCode() + key.getAccessType().hashCode();
    }
  }
}

