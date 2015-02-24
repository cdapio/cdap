/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.app.guice;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedProcedureProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import co.cask.cdap.internal.app.runtime.distributed.DistributedServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedWebappProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
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
    runnerFactoryBinder.addBinding(ProgramRunnerFactory.Type.WORKER).to(DistributedWorkerProgramRunner.class);

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
