/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.guice;

import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.common.LocalLogWriter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.runtime.flow.FlowProgramRunner;
import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.service.InMemoryProgramRuntimeService;
import com.continuuity.internal.app.services.DefaultAppFabricService;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

import java.util.Map;

/**
 *
 */
public class BigMamaModule extends AbstractModule {

  private final CConfiguration configuration;

  public BigMamaModule(CConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Configures a {@link com.google.inject.Binder} via the exposed methods.
   */
  @Override
  protected void configure() {

    // Bind config
    bind(CConfiguration.class).toInstance(configuration);

    // Bind LogWriter
    bind(LogWriter.class).toInstance(new LocalLogWriter(configuration));

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(FlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOWLET).to(FlowletProgramRunner.class);

    bind(ProgramRunnerFactory.class).to(InMemoryFlowProgramRunnerFactory.class).in(Scopes.SINGLETON);

    // Bind runtime service
    bind(ProgramRuntimeService.class).to(InMemoryProgramRuntimeService.class).in(Scopes.SINGLETON);

    bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class);

    bind(LocationFactory.class).to(LocalLocationFactory.class);
    bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
    bind(ManagerFactory.class).to(SyncManagerFactory.class);
    bind(StoreFactory.class).to(MDSStoreFactory.class);
    bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
    bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
    bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
    bind(AppFabricService.Iface.class).to(DefaultAppFabricService.class);
  }


  @Singleton
  private static final class InMemoryFlowProgramRunnerFactory implements ProgramRunnerFactory {

    private final Map<ProgramRunnerFactory.Type, Provider<ProgramRunner>> providers;

    @Inject
    private InMemoryFlowProgramRunnerFactory(Map<ProgramRunnerFactory.Type, Provider<ProgramRunner>> providers) {
      this.providers = providers;
    }

    @Override
    public ProgramRunner create(ProgramRunnerFactory.Type programType) {
      Provider<ProgramRunner> provider = providers.get(programType);
      Preconditions.checkNotNull(provider, "Unsupported program type: " + programType);
      return provider.get();
    }
  }
}
