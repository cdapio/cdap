/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProcedureProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Map;

/**
 * Guice module for distributed AppFabric. Used by the app-fabric server, not for distributed containers.
 */
final class DistributedProgramRunnerModule extends PrivateModule {

  @Override
  protected void configure() {
    // Bind and expose WeaveRunner
    bind(WeaveRunner.class).to(YarnWeaveRunnerService.class);
    bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);

    expose(WeaveRunner.class);
    expose(WeaveRunnerService.class);

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(DistributedFlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.PROCEDURE).to(DistributedProcedureProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);

    // Bind and expose ProgramRuntimeService
    bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class);
    expose(ProgramRuntimeService.class);
  }

  @Singleton
  @Provides
  private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration) {
    String namespace = configuration.get("weave.zookeeper.namespace", "/weave");
    return new YarnWeaveRunnerService(yarnConfiguration, configuration.get("zookeeper.quorum") + namespace);
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
