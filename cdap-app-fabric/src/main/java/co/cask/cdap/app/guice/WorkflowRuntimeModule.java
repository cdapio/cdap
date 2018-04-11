/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.proto.ProgramType;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;

/**
 * A guice module to provide bindings for {@link ProgramRunnerFactory} used by Workflow execution.
 */
public class WorkflowRuntimeModule extends PrivateModule {

  @Override
  protected void configure() {
    // Bind ProgramRunner for MR, which is used by Workflow.
    // The ProgramRunner for Spark is provided by the DefaultProgramRunnerFactory through the extension mechanism
    MapBinder<ProgramType, ProgramRunner> runnerFactoryBinder =
      MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
    runnerFactoryBinder.addBinding(ProgramType.MAPREDUCE).to(MapReduceProgramRunner.class);

    // It uses local mode factory because for Workflow we launch the job from the Workflow container directly.
    // The actual execution mode of the job is governed by the framework configuration
    // For mapreduce, it's in the mapred-site.xml
    // for spark, it's in the hConf we shipped from DistributedWorkflowProgramRunner
    bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.LOCAL);
    bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
    expose(ProgramRunnerFactory.class);
  }
}
