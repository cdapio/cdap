package com.continuuity.internal.app.runtime.batch.inmemory;

import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Builds an instance of {@link com.continuuity.internal.app.runtime.batch.BasicMapReduceContext} good for
 * in-memory environment
 */
public class InMemoryMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;
  private final TaskAttemptContext taskContext;

  public InMemoryMapReduceContextBuilder(CConfiguration cConf, TaskAttemptContext taskContext) {
    this.cConf = cConf;
    this.taskContext = taskContext;
  }

  @Override
  protected Injector prepare() {
    // TODO: this logic should go into DataFabricModules. We'll move it once Guice modules are refactored
    Constants.InMemoryPersistenceType persistenceType = Constants.InMemoryPersistenceType.valueOf(
      cConf.get(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.DEFAULT_DATA_INMEMORY_PERSISTENCE));

    if (Constants.InMemoryPersistenceType.MEMORY == persistenceType) {
      return createInMemoryModules();
    } else {
      return createPersistentModules();
    }
  }

  private Injector createInMemoryModules() {
    ImmutableList<Module> inMemoryModules = ImmutableList.of(
      new ConfigModule(cConf),
      new LocalConfigModule(),
      new IOModule(),
      new AuthModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModule(),
      new MetricsClientRuntimeModule().getNoopModules(),
      new LoggingModules().getInMemoryModules()
    );

    return Guice.createInjector(inMemoryModules);
  }

  private Injector createPersistentModules() {
    ImmutableList<Module> singleNodeModules = ImmutableList.of(
      new ConfigModule(cConf),
      new LocalConfigModule(),
      new IOModule(),
      new AuthModule(),
      new LocationRuntimeModule().getSingleNodeModules(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new ProgramRunnerRuntimeModule().getSingleNodeModules(),
      new DataFabricModules().getSingleNodeModules(),
      new DataSetsModules().getLocalModule(),
      new MetricsClientRuntimeModule().getMapReduceModules(taskContext),
      new LoggingModules().getSingleNodeModules()
    );
    return Guice.createInjector(singleNodeModules);
  }

  /**
   * Provides bindings to configs needed by other modules binding in this class.
   */
  private static class LocalConfigModule extends AbstractModule {

    @Override
    protected void configure() {
      // No-op
    }

    @Provides
    @Named(Constants.AppFabric.SERVER_ADDRESS)
    public InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.AppFabric.SERVER_ADDRESS),
                              new InetSocketAddress("localhost", 0).getAddress());
    }
  }
}
