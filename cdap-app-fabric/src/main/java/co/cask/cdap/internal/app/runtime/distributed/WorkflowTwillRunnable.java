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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.spark.SparkProgramRunner;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Modules;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;
import org.apache.twill.api.TwillContext;

import java.util.Map;

/**
 *
 */
final class WorkflowTwillRunnable extends AbstractProgramTwillRunnable<WorkflowProgramRunner> {

  // NOTE: DO NOT REMOVE.  Though it is unused, the dependency is needed when submitting the mapred job.
  @SuppressWarnings("unused")
  private YarnClientProtocolProvider provider;

  WorkflowTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WorkflowProgramRunner> getProgramClass() {
    return WorkflowProgramRunner.class;
  }

  @Override
  protected Module createModule(TwillContext context) {
    Module module = super.createModule(context);
    return Modules.combine(module, new PrivateModule() {
      @Override
      protected void configure() {
        // Bind ProgramRunner for MR and Spark, which is used by Workflow
        MapBinder<ProgramType, ProgramRunner> runnerFactoryBinder =
          MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
        runnerFactoryBinder.addBinding(ProgramType.MAPREDUCE).to(MapReduceProgramRunner.class);
        runnerFactoryBinder.addBinding(ProgramType.SPARK).to(SparkProgramRunner.class);

        bind(ProgramRunnerFactory.class).to(WorkflowProgramRunnerFactory.class).in(Scopes.SINGLETON);
        expose(ProgramRunnerFactory.class);
      }
    });
  }

  @Override
  protected boolean propagateServiceError() {
    // Don't propagate Workflow failure as failure. Quick fix for CDAP-749.
    return false;
  }

  @Singleton
  private static final class WorkflowProgramRunnerFactory implements ProgramRunnerFactory {

    private final Map<ProgramType, Provider<ProgramRunner>> providers;

    @Inject
    private WorkflowProgramRunnerFactory(Map<ProgramType, Provider<ProgramRunner>> providers) {
      this.providers = providers;
    }

    @Override
    public ProgramRunner create(ProgramType programType) {
      Provider<ProgramRunner> provider = providers.get(programType);
      Preconditions.checkNotNull(provider, "Unsupported program type: " + programType);
      return provider.get();
    }
  }
}
