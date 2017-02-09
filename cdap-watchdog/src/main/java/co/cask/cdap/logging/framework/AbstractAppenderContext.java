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

package co.cask.cdap.logging.framework;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DefaultDatasetManager;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Collections;

/**
 * The base implementation of {@link AppenderContext} that provides integration with CDAP system.
 */
public abstract class AbstractAppenderContext extends AppenderContext {

  private final Transactional transactional;
  private final LocationFactory locationFactory;
  private final DatasetManager datasetManager;
  private final MetricsContext metricsContext;

  protected AbstractAppenderContext(DatasetFramework datasetFramework,
                                    TransactionSystemClient txClient,
                                    LocationFactory locationFactory,
                                    MetricsCollectionService metricsCollectionService) {
    this.locationFactory = locationFactory;
    // No need to have retry for dataset admin operations
    // The log framework itself will perform retries and state management
    this.datasetManager = new DefaultDatasetManager(datasetFramework, NamespaceId.SYSTEM,
                                                    co.cask.cdap.common.service.RetryStrategies.noRetry());
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.metricsContext = metricsCollectionService.getContext(Collections.<String, String>emptyMap());
  }

  @Override
  public final DatasetManager getDatasetManager() {
    return datasetManager;
  }

  @Override
  public final LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public final void execute(TxRunnable runnable) throws TransactionFailureException {
    transactional.execute(runnable);
  }

  @Override
  public final void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
    transactional.execute(timeoutInSeconds, runnable);
  }

  @Override
  public final MetricsContext getMetricsContext() {
    return metricsContext;
  }
}
