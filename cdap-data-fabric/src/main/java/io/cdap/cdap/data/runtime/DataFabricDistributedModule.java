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

package io.cdap.cdap.data.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.transaction.DistributedTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.data2.transaction.metrics.TransactionManagerMetricsCollector;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.PooledClientProvider;
import org.apache.tephra.distributed.ThreadLocalClientProvider;
import org.apache.tephra.distributed.ThriftClientProvider;
import org.apache.tephra.distributed.TransactionServiceClient;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.persist.HDFSTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.runtime.TransactionStateStorageProvider;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines guice bindings for distributed modules.
 */
public class DataFabricDistributedModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DataFabricDistributedModule.class);
  private final String txClientId;

  public DataFabricDistributedModule(String txClientId) {
    this.txClientId = txClientId;
  }

  @Override
  public void configure() {
    bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);
    bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class).in(Scopes.SINGLETON);

    // bind transactions
    bind(TransactionSystemClientService.class).to(DistributedTransactionSystemClientService.class);

    // some of these classes need to be non-singleton in order to create a new instance during leader() in
    // TransactionService
    bind(SnapshotCodecProvider.class).in(Scopes.SINGLETON);
    bind(TransactionStateStorage.class).annotatedWith(Names.named("persist")).to(HDFSTransactionStateStorage.class);
    bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class);
    // to catch issues during configure time
    bind(TransactionManager.class);

    bindConstant().annotatedWith(Names.named(TxConstants.CLIENT_ID)).to(txClientId);

    bind(TransactionSystemClient.class).toProvider(DistributedTxSystemClientProvider.class).in(Scopes.SINGLETON);
    bind(MetricsCollector.class).to(TransactionManagerMetricsCollector.class).in(Scopes.SINGLETON);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));

    install(new TransactionExecutorModule());
    install(new StorageModule());
  }

  /**
   * Provides implementation of {@link ThriftClientProvider} based on configuration.
   */
  @Singleton
  public static final class ThriftClientProviderSupplier implements Provider<ThriftClientProvider> {

    private final CConfiguration cConf;
    private final Configuration hConf;
    private DiscoveryServiceClient discoveryServiceClient;

    @Inject
    ThriftClientProviderSupplier(CConfiguration cConf, Configuration hConf) {
      this.cConf = cConf;
      this.hConf = hConf;
    }

    @Inject(optional = true)
    void setDiscoveryServiceClient(DiscoveryServiceClient discoveryServiceClient) {
      this.discoveryServiceClient = discoveryServiceClient;
    }

    @Override
    public ThriftClientProvider get() {
      // configure the client provider
      String provider = cConf.get(TxConstants.Service.CFG_DATA_TX_CLIENT_PROVIDER,
                                  TxConstants.Service.DEFAULT_DATA_TX_CLIENT_PROVIDER);
      ThriftClientProvider clientProvider;
      if ("pool".equals(provider)) {
        clientProvider = new PooledClientProvider(hConf, discoveryServiceClient);
      } else if ("thread-local".equals(provider)) {
        clientProvider = new ThreadLocalClientProvider(hConf, discoveryServiceClient);
      } else {
        String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
        LOG.error(message);
        throw new IllegalArgumentException(message);
      }
      return clientProvider;
    }
  }

  /**
   * Distributed transaction client provider which provides the {@link TransactionSystemClient} for distributed mode.
   */
  private static final class DistributedTxSystemClientProvider extends AbstractTransactionSystemClientProvider {
    private final Injector injector;

    @Inject
    DistributedTxSystemClientProvider(CConfiguration cConf, Injector injector) {
      super(cConf);
      this.injector = injector;
    }

    @Override
    protected TransactionSystemClient getTransactionSystemClient() {
      return injector.getInstance(TransactionServiceClient.class);
    }
  }
}
