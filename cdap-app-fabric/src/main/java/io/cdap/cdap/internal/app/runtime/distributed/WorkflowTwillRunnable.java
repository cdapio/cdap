/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Modules;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.app.guice.UnsupportedPluginFinder;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import io.cdap.cdap.metadata.LocalPreferencesFetcherInternal;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillRunnable;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link TwillRunnable} for running a workflow driver.
 */
public final class WorkflowTwillRunnable extends AbstractProgramTwillRunnable<WorkflowProgramRunner> {

  public WorkflowTwillRunnable(String name) {
    super(name);
  }

  @Override
  protected Module createModule(CConfiguration cConf, Configuration hConf,
                                ProgramOptions programOptions, ProgramRunId programRunId) {
    List<Module> modules = new ArrayList<>();

    modules.add(super.createModule(cConf, hConf, programOptions, programRunId));
    if (ProgramRunners.getClusterMode(programOptions) == ClusterMode.ON_PREMISE) {
      modules.add(new DistributedArtifactManagerModule());
    } else {
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          bind(PluginFinder.class).to(UnsupportedPluginFinder.class);
          //TODO: Remove if needed
          //bind(PreferencesFetcher.class).to(LocalPreferencesFetcherInternal.class);
        }
      });
    }

    modules.add(new PrivateModule() {
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
    });

    return Modules.combine(modules);
  }
}
