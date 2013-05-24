package com.continuuity.internal.app.runtime.batch.distributed;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.internal.filesystem.HDFSLocationFactory;
import com.continuuity.runtime.MetadataModules;
import com.continuuity.runtime.MetricsModules;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;

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

  protected Injector createInjector() {
    ImmutableList<Module> modules = ImmutableList.of(
      new BigMamaModule(cConf),
      new MetricsModules().getSingleNodeModules(),
      new MetadataModules().getSingleNodeModules(),
      new Module() {
        @Override
        public void configure(Binder binder) {
          binder.bind(Configuration.class).toInstance(hConf);

          // Data-fabric bindings
          // this makes mapreduce tasks talk to HBase/HDFS directly
          binder.bind(OperationExecutor.class).
            to(OmidTransactionalOperationExecutor.class).in(Singleton.class);

          binder.bind(OVCTableHandle.class).to(HBaseOVCTableHandle.class);

          binder.bind(TimestampOracle.class).to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);
          binder.bind(TransactionOracle.class).to(MemoryOracle.class).in(Singleton.class);

          binder.bind(Configuration.class).annotatedWith(Names.named("HBaseOVCTableHandleHConfig")).toInstance(hConf);
          binder.bind(CConfiguration.class).annotatedWith(Names.named("HBaseOVCTableHandleCConfig")).toInstance(cConf);

          // Every mr task talks to datastore directly bypassing oracle
          binder.bind(boolean.class).annotatedWith(Names.named("DataFabricOperationExecutorTalksToOracle"))
            .toInstance(false);
          binder.bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig"))
            .toInstance(cConf);
        }
      }
    );

    // We need the hack to override the binding in massive BigMamaModule
    // TODO: remove this hack after refactoring Guice modules (separate task that is in progress as of writing this)
    Module module = Modules.override(modules).with(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(LocationFactory.class).to(HDFSLocationFactory.class);
      }
    });


    return Guice.createInjector(module);
  }
}
