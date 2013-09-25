package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import com.continuuity.data2.transaction.server.TalkingToOpexTxSystemClient;
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

  private static final Logger Log =
      LoggerFactory.getLogger(DataFabricDistributedModule.class);

  private final CConfiguration conf;

  private final Configuration hbaseConf;

  /**
   * Create a module with default configuration for HBase and Continuuity.
   */
  public DataFabricDistributedModule() {
    this(CConfiguration.create(), HBaseConfiguration.create());
  }

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

    // Bind our implementations

    // Bind remote operation executor
    bind(OperationExecutor.class).to(RemoteOperationExecutor.class).in(Singleton.class);

    // For data fabric, bind to Omid and HBase
    bind(OperationExecutor.class).annotatedWith(Names.named("DataFabricOperationExecutor"))
        .to(OmidTransactionalOperationExecutor.class).in(Singleton.class);

    // Bind HBase configuration into ovctable
    bind(Configuration.class).annotatedWith(Names.named("HBaseOVCTableHandleHConfig")).toInstance(hbaseConf);

    // Bind Continuuity configuration into ovctable
    bind(CConfiguration.class).annotatedWith(Names.named("HBaseOVCTableHandleCConfig")).toInstance(conf);

    // Bind our configurations
    bind(CConfiguration.class).annotatedWith(Names.named("RemoteOperationExecutorConfig")).toInstance(conf);
    bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig")).toInstance(conf);

    // bind meta data store
    bind(MetaDataStore.class).to(SerializingMetaDataStore.class).in(Singleton.class);

    // Bind TxDs2 stuff
    if (conf.getBoolean(Constants.TransactionManager.CFG_DO_PERSIST, true)) {
      bind(TransactionStateStorage.class).to(HDFSTransactionStateStorage.class).in(Singleton.class);
    } else {
      bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Singleton.class);
    }
    bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);
    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(TalkingToOpexTxSystemClient.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }

  public CConfiguration getConfiguration() {
    return this.conf;
  }

}
