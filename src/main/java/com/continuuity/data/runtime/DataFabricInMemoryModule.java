/*
 * com.continuuity.data.runtime - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.data.table.handles.MemoryOVCTableHandle;
import com.google.inject.AbstractModule;

import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Singleton;

/**
 * DataFabricInMemoryModule defines the InMemory bindings for the data fabric.
 */
public class DataFabricInMemoryModule extends AbstractModule {

  public void configure() {

    // Bind our implementations

    // There is only one timestamp oracle for the whole system
    bind(TimestampOracle.class).
        to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);

    bind(TransactionOracle.class).to(MemoryOracle.class);

    bind(OVCTableHandle.class).to(MemoryOVCTableHandle.class);
    bind(OperationExecutor.class).
        to(OmidTransactionalOperationExecutor.class).in(Singleton.class);

  }

} // end of GatewayProductionModule
