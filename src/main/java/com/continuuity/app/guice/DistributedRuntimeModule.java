/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import com.continuuity.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import com.continuuity.internal.app.runtime.service.InMemoryProgramRuntimeService;
import com.continuuity.internal.discovery.ZKDiscoveryService;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.internal.yarn.YarnWeaveRunnerService;
import com.continuuity.zookeeper.DiscoveryService;
import com.continuuity.zookeeper.DiscoveryServiceClient;
import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Map;

/**
 * Guice module for distributed AppFabric. Used by the app-fabric server, not for distributed containers.
 */
public final class DistributedRuntimeModule extends PrivateModule {

  private final Configuration hConfiguration;
  private final CConfiguration cConfiguration;
  private final YarnConfiguration yarnConfiguration;

  public DistributedRuntimeModule(Configuration hConfiguration,
                                  CConfiguration cConfiguration,
                                  YarnConfiguration yarnConfiguration) {
    this.hConfiguration = hConfiguration;
    this.cConfiguration = cConfiguration;
    this.yarnConfiguration = yarnConfiguration;
  }

  @Override
  protected void configure() {
    // TODO(terence): Need to refactor it to smaller piece together with DataFabric modules

    // Bind configurations
    bind(Configuration.class).toInstance(hConfiguration);
    bind(CConfiguration.class).annotatedWith(Names.named("appfabric.config")).toInstance(cConfiguration);
    bind(YarnConfiguration.class).toInstance(yarnConfiguration);

    // Bind and expose DiscoveryService
    bind(DiscoveryService.class).to(ZKDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(ZKDiscoveryService.class);

    expose(DiscoveryService.class);
    expose(DiscoveryServiceClient.class);


    // Bind and expose WeaveRunner
    bindConstant().annotatedWith(Names.named("config.weave.namespace")).to("/weave");
    bind(WeaveRunner.class).to(YarnWeaveRunnerService.class);
    bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);

    expose(WeaveRunner.class);
    expose(WeaveRunnerService.class);


    // Bind ProgramRunner
    MapBinder<ProgramRunnerFactory.Type, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramRunnerFactory.Type.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.FLOW).to(DistributedFlowProgramRunner.class);

    // Bind and expose ProgramRuntimeService
//    bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class);
    bind(ProgramRuntimeService.class).to(InMemoryProgramRuntimeService.class);
    expose(ProgramRuntimeService.class);
  }

  /**
   * Singleton provider for {@link ZKDiscoveryService}.
   */
  @Singleton
  @Provides
  private ZKDiscoveryService provideZKDiscoveryService(@Named("appfabric.config") CConfiguration configuration) {
    return new ZKDiscoveryService(configuration.get("zookeeper.quorum"));
  }

  @Singleton
  @Provides
  private YarnWeaveRunnerService provideYarnWeaveRunnerService(@Named("config.weave.namespace") String namespace,
                                                               @Named("appfabric.config") CConfiguration configuration,
                                                               YarnConfiguration yarnConfiguration) {
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
