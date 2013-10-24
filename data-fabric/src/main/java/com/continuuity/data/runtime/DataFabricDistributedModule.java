package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.distributed.TransactionServiceClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import com.continuuity.data2.transaction.queue.hbase.HBaseStreamAdmin;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metadata.SerializingMetaDataTable;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
    bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);
    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(TransactionServiceClient.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
    bind(StreamAdmin.class).to(HBaseStreamAdmin.class).in(Singleton.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }

  public CConfiguration getConfiguration() {
    return this.conf;
  }

}
