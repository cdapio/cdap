/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.distributed.DistributedFlowProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import co.cask.cdap.internal.app.runtime.distributed.DistributedServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import co.cask.cdap.proto.ProgramType;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import org.apache.twill.api.TwillRunner;

/**
 * Guice module for distributed AppFabric. Used by the app-fabric server, not for distributed containers.
 */
final class DistributedProgramRunnerModule extends PrivateModule {

  @Override
  protected void configure() {

    // Bind ProgramRunnerFactory and expose it
    // ProgramRunnerFactory should be in distributed mode
    bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
    // Bind and expose ProgramRunnerFactory. It is used in both program deployment and program execution.
    // Should get refactory by CDAP-5506
    bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
    expose(ProgramRunnerFactory.class);


    // The following are bindings are for ProgramRunners. They are private to this module and only
    // available to the remote exeuction ProgramRunnerFactory exposed.

    // This set of program runners are for on_premise mode
    bind(ClusterMode.class).toInstance(ClusterMode.ON_PREMISE);
    // TwillRunner used by the ProgramRunner is the remote execution one
    bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class).to(TwillRunner.class);
    // ProgramRunnerFactory used by ProgramRunner is the remote execution one.
    bind(ProgramRunnerFactory.class)
      .annotatedWith(Constants.AppFabric.ProgramRunner.class)
      .to(ProgramRunnerFactory.class);

    // Bind ProgramRunner
    MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder =
      MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.FLOW).to(DistributedFlowProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.SERVICE).to(DistributedServiceProgramRunner.class);
    defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);

    // Bind and expose ProgramRuntimeService
    bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class).in(Scopes.SINGLETON);
    expose(ProgramRuntimeService.class);
  }
}
