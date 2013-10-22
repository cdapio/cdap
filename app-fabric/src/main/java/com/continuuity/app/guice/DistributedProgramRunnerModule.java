/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProcedureProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import com.continuuity.internal.app.runtime.distributed.DistributedWebappProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
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
    bind(WeaveRunnerService.class).to(AppFabricWeaveRunnerService.class);
    bind(WeaveRunner.class).to(WeaveRunnerService.class);

    expose(WeaveRunner.class);
    expose(WeaveRunnerService.class);

    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(DistributedFlowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.PROCEDURE).to(DistributedProcedureProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WEBAPP).to(DistributedWebappProgramRunner.class);

    // Bind and expose ProgramRuntimeService
    bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class).in(Scopes.SINGLETON);
    expose(ProgramRuntimeService.class);
  }

  @Singleton
  @Provides
  private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration,
                                                               LocationFactory locationFactory) {
    String zkConnectStr = configuration.get(Constants.Zookeeper.QUORUM) +
                          configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");

    // Copy the yarn config and set the max heap ratio.
    YarnConfiguration yarnConfig = new YarnConfiguration(yarnConfiguration);
    yarnConfig.set(Constants.CFG_WEAVE_RESERVED_MEMORY_MB, configuration.get(Constants.CFG_WEAVE_RESERVED_MEMORY_MB));
    YarnWeaveRunnerService runner = new YarnWeaveRunnerService(yarnConfig,
                                                               zkConnectStr,
                                                               LocationFactories.namespace(locationFactory, "weave"));

    runner.setJVMOptions(configuration.get(Constants.AppFabric.PROGRAM_JVM_OPTS));
    return runner;
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
