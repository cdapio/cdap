package com.continuuity.internal.app.runtime.batch.inmemory;

import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.program.Program;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import com.continuuity.runtime.MetadataModules;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;

import java.io.IOException;

/**
 * Builds an instance of {@link com.continuuity.internal.app.runtime.batch.BasicMapReduceContext} good for
 * in-memory environment
 */
public class InMemoryMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;

  public InMemoryMapReduceContextBuilder(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  protected Program loadProgram(String programLocation, LocationFactory locationFactory) throws IOException {
    return new Program(locationFactory.create(programLocation));
  }

  protected Injector createInjector() {
    // TODO: this logic should go into DataFabricModules. We'll move it once Guice modules are refactored
    Constants.InMemoryPersistenceType persistenceType = Constants.InMemoryPersistenceType.valueOf(
      cConf.get(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.DEFAULT_DATA_INMEMORY_PERSISTENCE));

    if (Constants.InMemoryPersistenceType.MEMORY == persistenceType) {
      return createInMemoryModules();
    } else {
      return createPersistentModules(persistenceType);
    }
  }

  private Injector createInMemoryModules() {
    ImmutableList<Module> inMemoryModules = ImmutableList.of(
      new ConfigModule(cConf),
      new IOModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new MetadataModules().getInMemoryModules(),
      // Every mr task talks to datastore directly bypassing oracle
      NoOracleOpexModule.INSTANCE
    );

    return Guice.createInjector(inMemoryModules);
  }

  private Injector createPersistentModules(Constants.InMemoryPersistenceType persistenceType) {
    ImmutableList<Module> singleNodeModules = ImmutableList.of(
      new ConfigModule(cConf),
      new IOModule(),
      new LocationRuntimeModule().getSingleNodeModules(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new ProgramRunnerRuntimeModule().getSingleNodeModules(),
      Constants.InMemoryPersistenceType.LEVELDB == persistenceType ?
        new DataFabricLevelDBModule(cConf) : new DataFabricModules().getSingleNodeModules(),
      new MetadataModules().getSingleNodeModules(),
      // Every mr task talks to datastore directly bypassing oracle
      NoOracleOpexModule.INSTANCE
    );
    return Guice.createInjector(singleNodeModules);
  }

  private static class NoOracleOpexModule extends AbstractModule {
    private static NoOracleOpexModule INSTANCE = new NoOracleOpexModule();

    @Override
    public void configure() {
      bind(boolean.class).annotatedWith(Names.named("DataFabricOperationExecutorTalksToOracle"))
        .toInstance(false);
    }
  }
}
