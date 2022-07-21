/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.common;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.nosql.NoSqlStorageProvider;
import io.cdap.cdap.spi.data.sql.PostgreSqlStorageProvider;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

/**
 * A {@link StorageProvider} that delegates to the actual storage provider implementation based on the configuration.
 */
public class DefaultStorageProvider implements StorageProvider {

  private final Injector injector;
  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final String storageImpl;
  private final StorageProviderExtensionLoader extensionLoader;
  private final MetricsCollector metricsCollector;
  private volatile StorageProvider delegate;

  @Inject
  DefaultStorageProvider(Injector injector, CConfiguration cConf, SConfiguration sConf,
                         StorageProviderExtensionLoader extensionLoader,
                         MetricsCollectionService metricsCollectionService) {
    this.injector = injector;
    this.cConf = cConf;
    this.sConf = sConf;
    this.storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
    this.extensionLoader = extensionLoader;
    this.metricsCollector = metricsCollectionService.getContext(Constants.Metrics.STORAGE_METRICS_TAGS);
  }

  @Override
  public void close() throws Exception {
    if (delegate != null) {
      delegate.close();
    }
  }

  @Override
  public String getName() {
    // The name doesn't matter, as it is not used
    return "system";
  }

  @Override
  public StructuredTableAdmin getStructuredTableAdmin() throws Exception {
    return getDelegate().getStructuredTableAdmin();
  }

  @Override
  public TransactionRunner getTransactionRunner() throws Exception {
    return getDelegate().getTransactionRunner();
  }

  /**
   * Returns the {@link StorageProvider} to use based on configuration.
   */
  private StorageProvider getDelegate() throws Exception {
    StorageProvider provider = this.delegate;
    if (provider != null) {
      return provider;
    }
    synchronized (this) {
      provider = this.delegate;
      if (provider != null) {
        return provider;
      }

      switch (storageImpl.toLowerCase()) {
        case Constants.Dataset.DATA_STORAGE_NOSQL:
          provider = injector.getInstance(NoSqlStorageProvider.class);
          break;
        case Constants.Dataset.DATA_STORAGE_SQL:
          provider = injector.getInstance(PostgreSqlStorageProvider.class);
          break;
        default:
          provider = extensionLoader.get(storageImpl);
      }

      if (provider == null) {
        throw new IllegalArgumentException("Unsupported storage implementation " + storageImpl);
      }

      provider.initialize(new DefaultStorageProviderContext(cConf, sConf, provider.getName(), metricsCollector));

      this.delegate = provider;
      return provider;
    }
  }
}
