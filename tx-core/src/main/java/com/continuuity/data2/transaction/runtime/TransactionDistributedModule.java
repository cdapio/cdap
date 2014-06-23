package com.continuuity.data2.transaction.runtime;

import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.distributed.TransactionServiceClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

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

    install(new FactoryModuleBuilder()
              .implement(TransactionExecutor.class, DefaultTransactionExecutor.class)
              .build(TransactionExecutorFactory.class));
  }
}
