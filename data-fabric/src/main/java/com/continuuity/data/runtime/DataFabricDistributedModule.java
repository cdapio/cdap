package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data.stream.DistributedStreamCoordinator;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.distributed.PooledClientProvider;
import com.continuuity.data2.transaction.distributed.ThreadLocalClientProvider;
import com.continuuity.data2.transaction.distributed.ThriftClientProvider;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import com.continuuity.data2.transaction.runtime.TransactionModules;
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
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines guice bindings for distributed modules.
 */
public class DataFabricDistributedModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(DataFabricDistributedModule.class);

  public DataFabricDistributedModule() {

  }

  @Override
  public void configure() {
    // bind meta data store
    bind(MetaDataTable.class).to(SerializingMetaDataTable.class).in(Singleton.class);

    bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);
    bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
    bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);

    // Stream bindings
    bind(StreamCoordinator.class).to(DistributedStreamCoordinator.class).in(Singleton.class);

    bind(StreamConsumerStateStoreFactory.class).to(HBaseStreamConsumerStateStoreFactory.class).in(Singleton.class);
    bind(StreamAdmin.class).to(HBaseStreamFileAdmin.class).in(Singleton.class);
    bind(StreamConsumerFactory.class).to(HBaseStreamFileConsumerFactory.class).in(Singleton.class);
    bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);

    // bind transactions
    install(new TransactionModules().getDistributedModules());

  }

  /**
   * Provides implementation of {@link ThriftClientProvider} based on configuration.
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
        LOG.error(message);
        throw new IllegalArgumentException(message);
      }
      return clientProvider;
    }
  }
}
