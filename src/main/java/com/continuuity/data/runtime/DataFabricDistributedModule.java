package com.continuuity.data.runtime;

import org.apache.hadoop.conf.Configuration;

import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

public class DataFabricDistributedModule extends AbstractModule {

  private final Configuration hbaseConf;
  
  public DataFabricDistributedModule() {
    this(new Configuration());
  }
  
  public DataFabricDistributedModule(Configuration hbaseConf) {
    this.hbaseConf = hbaseConf;
  }

  @Override
  public void configure() {

    // Bind our implementations

    // There is only one timestamp oracle for the whole system
    bind(TimestampOracle.class).
        to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);

    bind(TransactionOracle.class).to(MemoryOracle.class);

    bind(OVCTableHandle.class).to(HBaseOVCTableHandle.class);
//    bind(ColumnarTableHandle.class).to(HBaseColumnarTableHandle.class);
    bind(OperationExecutor.class).
        to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
    
    // Bind named fields
    
    bind(Configuration.class)
        .annotatedWith(Names.named("HBaseOVCTableHandleConfig"))
        .toInstance(hbaseConf);

  }

}
