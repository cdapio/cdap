/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProcedureProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import com.continuuity.internal.app.runtime.distributed.DistributedServiceProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedWebappProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;

import java.util.Map;

/**
 * Guice module for distributed AppFabric. Used by the app-fabric server, not for distributed containers.
 */
final class DistributedProgramRunnerModule extends PrivateModule {

  @Override
  protected void configure() {
    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(DistributedFlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.PROCEDURE).to(DistributedProcedureProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WEBAPP).to(DistributedWebappProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.SERVICE).to(DistributedServiceProgramRunner.class);

    // Bind and expose ProgramRuntimeService
    bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class).in(Scopes.SINGLETON);
    expose(ProgramRuntimeService.class);
  }

  @Singleton
  @Provides
  private ProgramRunnerFactory provideProgramRunnerFactory(final Map<ProgramRunnerFactory.Type,
                                                                     Provider<ProgramRunner>> providers) {

    return new ProgramRunnerFactory() {
      @Override
      public ProgramRunner create(Type programType) {
        Provider<ProgramRunner> provider = providers.get(programType);
        Preconditions.checkNotNull(provider, "Unsupported program type: " + programType);
        return provider.get();
      }
    };
  }
}
