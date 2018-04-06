/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.transaction.MultiThreadTransactionAware;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Implementation of {@link DynamicDatasetCache} that performs all operations on a per-thread basis.
 * That is, every thread is guaranteed to receive its own distinct copy of every dataset; every thread
 * has its own transaction context, etc.
 */
public class MultiThreadDatasetCache extends DynamicDatasetCache {

  // maintains a single threaded factory for each thread.
  private final LoadingCache<Thread, SingleThreadDatasetCache> perThreadMap;

  /**
   * See {@link DynamicDatasetCache}.
   *
   * @param staticDatasets  if non-null, a map from dataset name to runtime arguments. These datasets will be
   *                        instantiated immediately, and they will participate in every transaction started
   *                        through {@link #newTransactionContext}.
   * @param multiThreadTxAwares a list of {@link MultiThreadTransactionAware} that will get added to each
   *                            {@link SingleThreadDatasetCache} instance created for each thread through the
   *                            {@link SingleThreadDatasetCache#addExtraTransactionAware(TransactionAware)} method
   *                            to participate in transaction lifecycle.
   */
  public MultiThreadDatasetCache(final SystemDatasetInstantiator instantiator,
                                 final TransactionSystemClient txClient,
                                 final NamespaceId namespace,
                                 final Map<String, String> runtimeArguments,
                                 @Nullable final MetricsContext metricsContext,
                                 @Nullable final Map<String, Map<String, String>> staticDatasets,
                                 final MultiThreadTransactionAware<?>...multiThreadTxAwares) {
    super(instantiator, txClient, namespace, runtimeArguments);
    this.perThreadMap = CacheBuilder.newBuilder()
      .weakKeys()
      .removalListener(new RemovalListener<Thread, DynamicDatasetCache>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<Thread, DynamicDatasetCache> notification) {
          DynamicDatasetCache cache = notification.getValue();
          if (cache != null) {
            cache.close();
          }
        }
      })
      .build(
        new CacheLoader<Thread, SingleThreadDatasetCache>() {
          @Override
          @ParametersAreNonnullByDefault
          public SingleThreadDatasetCache load(Thread thread) throws Exception {
            SingleThreadDatasetCache cache = new SingleThreadDatasetCache(
              instantiator, txClient, namespace, runtimeArguments, metricsContext, staticDatasets);
            for (MultiThreadTransactionAware<?> txAware : multiThreadTxAwares) {
              cache.addExtraTransactionAware(txAware);
            }
            return cache;
          }
        });
  }

  @Override
  public void invalidate() {
    // note that this only invalidates the datasets for the current thread, whereas close() invalidates all
    entryForCurrentThread().invalidate();
  }

  @Override
  public void close() {
    super.close();
    perThreadMap.invalidateAll();
  }

  @Override
  public <T extends Dataset> T getDataset(DatasetCacheKey key, boolean bypass)
    throws DatasetInstantiationException {
    return entryForCurrentThread().getDataset(key, bypass);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    entryForCurrentThread().discardDataset(dataset);
  }

  @Override
  public TransactionContext newTransactionContext() throws TransactionFailureException {
    return entryForCurrentThread().newTransactionContext();
  }

  @Override
  public void dismissTransactionContext() {
    entryForCurrentThread().dismissTransactionContext();
  }

  @Override
  public Iterable<TransactionAware> getStaticTransactionAwares() {
    return entryForCurrentThread().getStaticTransactionAwares();
  }

  @Override
  public Iterable<TransactionAware> getTransactionAwares() {
    return entryForCurrentThread().getTransactionAwares();
  }

  @Override
  public Iterable<TransactionAware> getExtraTransactionAwares() {
    return entryForCurrentThread().getExtraTransactionAwares();
  }

  @Override
  public void addExtraTransactionAware(TransactionAware txAware) {
    entryForCurrentThread().addExtraTransactionAware(txAware);
  }

  @Override
  public void removeExtraTransactionAware(TransactionAware txAware) {
    entryForCurrentThread().removeExtraTransactionAware(txAware);
  }

  private DynamicDatasetCache entryForCurrentThread() {
    try {
      return perThreadMap.get(Thread.currentThread());
    } catch (ExecutionException e) {
      // this should never happen because all we do in the cache loader is crete a new entry.
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  public Collection<Thread> getCacheKeys() {
    perThreadMap.cleanUp();
    return perThreadMap.asMap().keySet();
  }
}
