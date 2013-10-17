/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryStreamAdmin;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metadata.SerializingMetaDataTable;
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

  @Override
  public Module getInMemoryModules() {
      return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetaDataTable.class).to(SerializingMetaDataTable.class).in(Singleton.class);

        // Bind TxDs2 stuff
        bind(DataSetAccessor.class).to(InMemoryDataSetAccessor.class).in(Singleton.class);
        bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Singleton.class);
        bind(InMemoryTransactionManager.class).in(Singleton.class);
        bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
        bind(QueueClientFactory.class).to(InMemoryQueueClientFactory.class).in(Singleton.class);
        bind(QueueAdmin.class).to(InMemoryQueueAdmin.class).in(Singleton.class);
        bind(StreamAdmin.class).to(InMemoryStreamAdmin.class).in(Singleton.class);

        // We don't need caching for in-memory
        bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig")).toInstance(cConf);
        bind(CConfiguration.class).annotatedWith(Names.named("DataSetAccessorConfig")).toInstance(cConf);
        bind(CConfiguration.class).annotatedWith(Names.named("TransactionServerConfig")).toInstance(cConf);

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
