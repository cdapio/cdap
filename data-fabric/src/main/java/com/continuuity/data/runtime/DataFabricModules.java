/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.Serializing2MetaDataStore;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.data2.transaction.inmemory.NoopPersistor;
import com.continuuity.data2.transaction.inmemory.StatePersistor;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * DataFabricModules defines all of the bindings for the different data
 * fabric modes.
 */
public class DataFabricModules extends RuntimeModule {
  private final CConfiguration cConf;
  private final Configuration hbaseConf;

  public DataFabricModules() {
    this(CConfiguration.create(), HBaseConfiguration.create());
  }

  public DataFabricModules(CConfiguration cConf) {
    this(cConf, HBaseConfiguration.create());
  }

  public DataFabricModules(CConfiguration cConf, Configuration hbaseConf) {
    this.cConf = cConf;
    this.hbaseConf = hbaseConf;
  }

  public Module getNoopModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(OperationExecutor.class).
            to(NoOperationExecutor.class).in(Singleton.class);
      }
    };
  }

  @Override
  public Module getInMemoryModules() {
    CConfiguration conf = CConfiguration.create();
    return getInMemoryModules(conf);
  }

  public Module getInMemoryModules(final CConfiguration conf) {

      return new AbstractModule() {
      @Override
      protected void configure() {
        bind(TimestampOracle.class).to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);
        bind(TransactionOracle.class).to(MemoryOracle.class).in(Singleton.class);
        bind(OVCTableHandle.class).toInstance(MemoryOVCTableHandle.getInstance());
        bind(OperationExecutor.class).to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
        bind(MetaDataStore.class).to(Serializing2MetaDataStore.class).in(Singleton.class);

        // Bind TxDs2 stuff
        bind(DataSetAccessor.class).to(InMemoryDataSetAccessor.class).in(Singleton.class);
        bind(StatePersistor.class).to(NoopPersistor.class).in(Singleton.class);
        bind(InMemoryTransactionManager.class).in(Singleton.class);
        bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
        bind(QueueClientFactory.class).to(InMemoryQueueClientFactory.class).in(Singleton.class);
        bind(QueueAdmin.class).to(InMemoryQueueAdmin.class).in(Singleton.class);

        // We don't need caching for in-memory
        cConf.setLong(Constants.CFG_QUEUE_STATE_PROXY_MAX_CACHE_SIZE_BYTES, 0);
        bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig"))
          .toInstance(cConf);

        install(new FactoryModuleBuilder()
                  .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
                  .build(TransactionExecutorFactory.class));
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new DataFabricLocalModule();
  }

  public Module getSingleNodeModules(CConfiguration conf) {
    return new DataFabricLocalModule(conf);
  }

  @Override
  public Module getDistributedModules() {
    return new DataFabricDistributedModule(cConf, hbaseConf);
  }

} // end of DataFabricModules
