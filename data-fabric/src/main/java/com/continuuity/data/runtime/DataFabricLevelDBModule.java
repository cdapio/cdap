/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.LocalDataSetAccessor;
import com.continuuity.data.stream.InMemoryStreamCoordinator;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.data2.transaction.persist.LocalFileTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBQueueAdmin;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBQueueClientFactory;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metadata.SerializingMetaDataTable;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLevelDBModule extends AbstractModule {

  @Override
  public void configure() {

    // bind meta data store
    bind(MetaDataTable.class).to(SerializingMetaDataTable.class).in(Singleton.class);

    // Bind TxDs2 stuff
    bind(LevelDBOcTableService.class).toInstance(LevelDBOcTableService.getInstance());

    bind(TransactionStateStorage.class).annotatedWith(Names.named("persist"))
      .to(LocalFileTransactionStateStorage.class).in(Singleton.class);
    bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class).in(Singleton.class);

    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
    bind(DataSetAccessor.class).to(LocalDataSetAccessor.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(LevelDBQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(LevelDBQueueAdmin.class).in(Singleton.class);

    // Stream bindings.
    bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Singleton.class);

    bind(StreamConsumerStateStoreFactory.class).to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
    bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
    bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
    bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }
}
