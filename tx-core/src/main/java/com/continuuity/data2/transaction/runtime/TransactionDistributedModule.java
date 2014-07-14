package com.continuuity.data2.transaction.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.distributed.PooledClientProvider;
import com.continuuity.data2.transaction.distributed.ThreadLocalClientProvider;
import com.continuuity.data2.transaction.distributed.ThriftClientProvider;
import com.continuuity.data2.transaction.distributed.TransactionServiceClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Guice bindings for running in distributed mode on a cluster.
 */
public class TransactionDistributedModule extends AbstractModule {

  public TransactionDistributedModule() {
  }

  @Override
  protected void configure() {
    bind(SnapshotCodecProvider.class).in(Singleton.class);
    bind(TransactionStateStorage.class).annotatedWith(Names.named("persist"))
      .to(HDFSTransactionStateStorage.class).in(Singleton.class);
    bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class).in(Singleton.class);

    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(TransactionServiceClient.class).in(Singleton.class);
    bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }

  /**
   * Provides implementation of {@link com.continuuity.data2.transaction.distributed.ThriftClientProvider}
   * based on configuration.
   */
  @Singleton
  private static final class ThriftClientProviderSupplier implements Provider<ThriftClientProvider> {

    private final CConfiguration cConf;
    private DiscoveryServiceClient discoveryServiceClient;

    @Inject
    ThriftClientProviderSupplier(CConfiguration cConf) {
      this.cConf = cConf;
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
        clientProvider = new PooledClientProvider(cConf, discoveryServiceClient);
      } else if ("thread-local".equals(provider)) {
        clientProvider = new ThreadLocalClientProvider(cConf, discoveryServiceClient);
      } else {
        String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
        throw new IllegalArgumentException(message);
      }
      return clientProvider;
    }
  }
}
