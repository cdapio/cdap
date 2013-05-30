package com.continuuity.internal.app.runtime.batch.distributed;

import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.program.Program;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

/**
 * Builds an instance of {@link com.continuuity.internal.app.runtime.batch.BasicMapReduceContext} good for
 * distributed environment. The context is to be used in remote worker (e.g. Mapper task started in YARN container)
 */
public class DistributedMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;
  private final Configuration hConf;

  public DistributedMapReduceContextBuilder(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  protected Program loadProgram(String programLocation, LocationFactory locationFactory) throws IOException {
    return new Program(locationFactory.create(URI.create(programLocation)));
  }

  protected Injector createInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new AbstractModule() {
        @Override
        protected void configure() {

          // Data-fabric bindings
          // this makes mapreduce tasks talk to HBase/HDFS directly
          bind(OperationExecutor.class).
            to(OmidTransactionalOperationExecutor.class).in(Singleton.class);

          bind(OVCTableHandle.class).to(HBaseOVCTableHandle.class);

          bind(TimestampOracle.class).to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);
          bind(TransactionOracle.class).to(MemoryOracle.class).in(Singleton.class);

          bind(Configuration.class).annotatedWith(Names.named("HBaseOVCTableHandleHConfig")).to(Configuration.class);
          bind(CConfiguration.class).annotatedWith(Names.named("HBaseOVCTableHandleCConfig")).to(CConfiguration.class);
          bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig")).to
            (CConfiguration.class);

          // Every mr task talks to datastore directly bypassing oracle
          bind(boolean.class).annotatedWith(Names.named("DataFabricOperationExecutorTalksToOracle"))
            .toInstance(false);
        }
      }
    );
  }
}
