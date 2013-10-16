/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.LocalDataSetAccessor;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
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
import com.continuuity.data2.transaction.queue.leveldb.LevelDBQueueAdmin;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBQueueClientFactory;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBStreamAdmin;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metadata.SerializingMetaDataTable;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

import java.io.File;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLevelDBModule extends AbstractModule {

  private final CConfiguration conf;

  public DataFabricLevelDBModule() {
    this(CConfiguration.create());
  }

  public DataFabricLevelDBModule(CConfiguration configuration) {
    String path = configuration.get(Constants.CFG_DATA_LEVELDB_DIR);
    if (path == null || path.isEmpty()) {
      path =
        System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") +
        "ldb-test-" + Long.toString(System.currentTimeMillis());
      configuration.set(Constants.CFG_DATA_LEVELDB_DIR, path);
    }

    File p = new File(path);
    if (!p.exists() && !p.mkdirs()) {
      throw new RuntimeException("Unable to create directory for ldb");
    }
    p.deleteOnExit();

    this.conf = configuration;
  }

  @Override
  public void configure() {

    // bind meta data store
    bind(MetaDataTable.class).to(SerializingMetaDataTable.class).in(Singleton.class);

    // Bind TxDs2 stuff
    bind(LevelDBOcTableService.class).toInstance(LevelDBOcTableService.getInstance());
    bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Singleton.class);
    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
    bind(CConfiguration.class).annotatedWith(Names.named("LevelDBConfiguration")).toInstance(conf);
    bind(CConfiguration.class).annotatedWith(Names.named("DataSetAccessorConfig")).toInstance(conf);
    bind(CConfiguration.class).annotatedWith(Names.named("TransactionServerConfig")).toInstance(conf);
    bind(DataSetAccessor.class).to(LocalDataSetAccessor.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(LevelDBQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(LevelDBQueueAdmin.class).in(Singleton.class);
    bind(StreamAdmin.class).to(LevelDBStreamAdmin.class).in(Singleton.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));

    bind(CConfiguration.class)
      .annotatedWith(Names.named("DataSetAccessorConfig"))
      .toInstance(conf);
  }
}
