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
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

/**
 * DataFabricModules defines all of the bindings for the different data
 * fabric modes.
 */
public class DataFabricModules extends RuntimeModule {

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

      return new AbstractModule() {
      @Override
      protected void configure() {
        CConfiguration conf = CConfiguration.create();
        bind(TimestampOracle.class).to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);
        bind(TransactionOracle.class).to(MemoryOracle.class).in(Singleton.class);
        bind(OVCTableHandle.class).toInstance(MemoryOVCTableHandle.getInstance());
        bind(OperationExecutor.class).to(OmidTransactionalOperationExecutor.class).in(Singleton.class);

        // Bind TxDs2 stuff
        bind(DataSetAccessor.class).to(InMemoryDataSetAccessor.class).in(Singleton.class);
        bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
        bind(QueueClientFactory.class).to(InMemoryQueueClientFactory.class).in(Singleton.class);

        // We don't need caching for in-memory
        conf.setLong(Constants.CFG_QUEUE_STATE_PROXY_MAX_CACHE_SIZE_BYTES, 0);
        bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig"))
          .toInstance(conf);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new DataFabricLocalModule();
  }

  @Override
  public Module getDistributedModules() {
    return new DataFabricDistributedModule();
  }

} // end of DataFabricModules
