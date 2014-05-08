package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.distributed.PooledClientProvider;
import com.continuuity.data2.transaction.distributed.ThreadLocalClientProvider;
import com.continuuity.data2.transaction.distributed.ThriftClientProvider;
import com.continuuity.data2.transaction.distributed.TransactionServiceClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.hbase.HBaseStreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.hbase.HBaseStreamFileAdmin;
import com.continuuity.data2.transaction.stream.hbase.HBaseStreamFileConsumerFactory;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metadata.SerializingMetaDataTable;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines guice bindings for distributed modules.
 */
public class DataFabricDistributedModule extends AbstractModule {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataFabricDistributedModule.class);

  private final CConfiguration conf;

  private final Configuration hbaseConf;

  /**
   * Create a module with custom configuration for HBase,
   * and defaults for Continuuity.
   */
  public DataFabricDistributedModule(Configuration conf) {
    this(CConfiguration.create(), conf);
  }

  /**
   * Create a module with custom configuration, which will
   * be used both for HBase and for Continuuity.
   */
  public DataFabricDistributedModule(CConfiguration conf) {
    this(conf, HBaseConfiguration.create());
  }

  /**
   * Create a module with custom configuration for HBase and Continuuity.
   */
  public DataFabricDistributedModule(CConfiguration conf, Configuration hbaseConf) {
    this.conf = conf;
    this.hbaseConf = hbaseConf;
  }

  @Override
  public void configure() {
    // Bind HBase configuration into ovctable
    bind(Configuration.class).annotatedWith(Names.named("HBaseOVCTableHandleHConfig")).toInstance(hbaseConf);

    // Bind Continuuity configuration into ovctable
    bind(CConfiguration.class).annotatedWith(Names.named("HBaseOVCTableHandleCConfig")).toInstance(conf);

    // Bind our configurations
    bind(CConfiguration.class).annotatedWith(Names.named("TransactionServerClientConfig")).toInstance(conf);
    bind(CConfiguration.class).annotatedWith(Names.named("TransactionServerConfig")).toInstance(conf);
    bind(CConfiguration.class).annotatedWith(Names.named("DataSetAccessorConfig")).toInstance(conf);

    // bind meta data store
    bind(MetaDataTable.class).to(SerializingMetaDataTable.class).in(Singleton.class);

    // Bind TxDs2 stuff
    if (conf.getBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, true)) {
      bind(TransactionStateStorage.class).to(HDFSTransactionStateStorage.class).in(Singleton.class);
    } else {
      bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Singleton.class);
    }
    bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);
    bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);
    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(TransactionServiceClient.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
    bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);

    bind(StreamConsumerStateStoreFactory.class).to(HBaseStreamConsumerStateStoreFactory.class).in(Singleton.class);
    bind(StreamAdmin.class).to(HBaseStreamFileAdmin.class).in(Singleton.class);
    bind(StreamConsumerFactory.class).to(HBaseStreamFileConsumerFactory.class).in(Singleton.class);
    bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }

  /**
   * Provides implementation of {@link ThriftClientProvider} based on configuration.
   */
  @Singleton
  private static final class ThriftClientProviderSupplier implements Provider<ThriftClientProvider> {

    private final CConfiguration cConf;
    private DiscoveryServiceClient discoveryServiceClient;

    @Inject
    ThriftClientProviderSupplier(@Named("TransactionServerClientConfig") CConfiguration cConf) {
      this.cConf = cConf;
    }

    @Inject(optional = true)
    void setDiscoveryServiceClient(DiscoveryServiceClient discoveryServiceClient) {
      this.discoveryServiceClient = discoveryServiceClient;
    }

    @Override
    public ThriftClientProvider get() {
      // configure the client provider
      String provider = cConf.get(Constants.Transaction.Service.CFG_DATA_TX_CLIENT_PROVIDER,
                                  Constants.Transaction.Service.DEFAULT_DATA_TX_CLIENT_PROVIDER);
      ThriftClientProvider clientProvider;
      if ("pool".equals(provider)) {
        clientProvider = new PooledClientProvider(cConf, discoveryServiceClient);
      } else if ("thread-local".equals(provider)) {
        clientProvider = new ThreadLocalClientProvider(cConf, discoveryServiceClient);
      } else {
        String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
        LOG.error(message);
        throw new IllegalArgumentException(message);
      }
      return clientProvider;
    }
  }

  public CConfiguration getConfiguration() {
    return this.conf;
  }

}
