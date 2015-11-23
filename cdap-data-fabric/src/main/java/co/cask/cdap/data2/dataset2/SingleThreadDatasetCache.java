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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.tx.WeakReferenceDelegatingTransactionAware;
import co.cask.cdap.data2.dataset2.tx.WeakReferencePromotingTransactionContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private final Map<DatasetCacheKey, WeakReferenceDelegatingTransactionAware> activeTxAwares = new HashMap<>();
  private final Map<DatasetCacheKey, Dataset> staticDatasets = new HashMap<>();
  private final Set<TransactionAware> extraTxAwares = Sets.newIdentityHashSet();

  private TransactionContext txContext = null;

  /**
   * See {@link DynamicDatasetCache}.
   *
   * @param staticDatasets  if non-null, a map from dataset name to runtime arguments. These datasets will be
   *                        instantiated immediately, and they will participate in every transaction started
   *                        through {@link #newTransactionContext()}.
   */
  public SingleThreadDatasetCache(final SystemDatasetInstantiator instantiator,
                                  final TransactionSystemClient txClient,
                                  final Id.Namespace namespace,
                                  Map<String, String> runtimeArguments,
                                  @Nullable final MetricsContext metricsContext,
                                  @Nullable Map<String, Map<String, String>> staticDatasets) {
    super(instantiator, txClient, namespace, runtimeArguments, metricsContext);
    this.datasetLoader = new CacheLoader<DatasetCacheKey, Dataset>() {
      @Override
      @ParametersAreNonnullByDefault
      public Dataset load(DatasetCacheKey key) throws Exception {
        Dataset dataset = instantiator.getDataset(
          Id.DatasetInstance.from(namespace, key.getName()), key.getArguments());
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
      .weakValues()
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
      WeakReferenceDelegatingTransactionAware txAware =
        new WeakReferenceDelegatingTransactionAware((TransactionAware) dataset);
      WeakReferenceDelegatingTransactionAware existing = activeTxAwares.get(key);
      if (existing == null) {
        activeTxAwares.put(key, txAware);
        if (txContext != null) {
          txContext.addTransactionAware(txAware);
        }
      } else {
        TransactionAware actual = existing.get();
        if (actual == null) {
          activeTxAwares.put(key, txAware);
          if (txContext != null) {
            txContext.removeTransactionAware(existing);
            txContext.addTransactionAware(txAware);
          }
        } else {
          // this better be the same dataset, otherwise the cache did not work
          if (actual != dataset) {
            throw new IllegalStateException(
              String.format("Unexpected state: Cache returned %s for %s, which is different from the " +
                              "active transaction aware %s for the same key. This should never happen.",
                            dataset, key, actual));
          }
        }
      }
    }
    return typedDataset;
  }

  @Override
  public TransactionContext newTransactionContext() {
    dismissTransactionContext();
    txContext = new WeakReferencePromotingTransactionContext(txClient);
    // make sure all in-progress participate in the transaction. But beware that weak refs may have expired
    List<DatasetCacheKey> expiredTxAwares = new ArrayList<>();
    for (Map.Entry<DatasetCacheKey, WeakReferenceDelegatingTransactionAware> entry : activeTxAwares.entrySet()) {
      if (entry.getValue().isExpired()) {
        expiredTxAwares.add(entry.getKey());
      } else {
        txContext.addTransactionAware(entry.getValue());
      }
    }
    // remove the expired dynamic datasets
    for (DatasetCacheKey key : expiredTxAwares) {
      activeTxAwares.remove(key);
    }
    // and finally also add the extra tx-awares
    for (TransactionAware txAware : extraTxAwares) {
      txContext.addTransactionAware(txAware);
    }
    return txContext;
  }

  @Override
  public void dismissTransactionContext() {
    txContext = null;
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
    return Iterables.concat(extraTxAwares, Iterables.filter(
      Iterables.transform(activeTxAwares.values(),
                          new Function<WeakReferenceDelegatingTransactionAware, TransactionAware>() {
                            @Nullable
                            public TransactionAware apply(WeakReferenceDelegatingTransactionAware ref) {
                              return ref.get();
                            }
                          }),
                          Predicates.notNull()));
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
}

