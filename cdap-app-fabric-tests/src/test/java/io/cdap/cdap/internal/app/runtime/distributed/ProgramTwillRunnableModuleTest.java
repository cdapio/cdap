/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.test.MockTwillContext;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.service.ServiceProgramRunner;
import io.cdap.cdap.internal.app.runtime.worker.WorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ServiceAnnouncer;
import org.junit.Test;

/**
 * Tests for guice modules used in various {@link AbstractProgramTwillRunnable}.
 */
public class ProgramTwillRunnableModuleTest {

  @Test
  public void testService() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").service("service").run(RunIds.generate());
    Module module = new ServiceTwillRunnable("service") {
      @Override
      protected ServiceAnnouncer getServiceAnnouncer() {
        return new MockTwillContext();
      }
    }.createModule(CConfiguration.create(), new Configuration(),
                   createProgramOptions(programRunId, ClusterMode.ON_PREMISE), programRunId);
    Injector injector = Guice.createInjector(module);
    injector.getInstance(ServiceProgramRunner.class);
    injector.getInstance(ExploreClient.class);
  }

  @Test
  public void testWorker() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").worker("worker").run(RunIds.generate());
    Module module = new WorkerTwillRunnable("worker").createModule(CConfiguration.create(), new Configuration(),
                                                                   createProgramOptions(programRunId,
                                                                                        ClusterMode.ON_PREMISE),
                                                                   programRunId);
    Injector injector = Guice.createInjector(module);
    injector.getInstance(WorkerProgramRunner.class);
    injector.getInstance(ExploreClient.class);
  }

  @Test
  public void testMapReduce() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").mr("mapreduce").run(RunIds.generate());
    for (ClusterMode mode : ClusterMode.values()) {
      Module module = new MapReduceTwillRunnable("mapreduce").createModule(CConfiguration.create(), new Configuration(),
                                                                           createProgramOptions(programRunId, mode),
                                                                           programRunId);
      Injector injector = Guice.createInjector(module);
      injector.getInstance(MapReduceProgramRunner.class);
      if (mode == ClusterMode.ON_PREMISE) {
        injector.getInstance(ExploreClient.class);
      }
    }
  }

  @Test
  public void testWorkflow() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").workflow("workflow").run(RunIds.generate());
    for (ClusterMode mode : ClusterMode.values()) {
      Module module = new WorkflowTwillRunnable("workflow").createModule(CConfiguration.create(), new Configuration(),
                                                                         createProgramOptions(programRunId, mode),
                                                                         programRunId);
      Injector injector = Guice.createInjector(module);
      injector.getInstance(WorkflowProgramRunner.class);
      // Workflow supports spark, which supports PluginFinder
      injector.getInstance(PluginFinder.class);

      if (mode == ClusterMode.ON_PREMISE) {
        injector.getInstance(ExploreClient.class);
      }
    }
  }

  private ProgramOptions createProgramOptions(ProgramRunId programRunId, ClusterMode clusterMode) {
    return new SimpleProgramOptions(programRunId.getParent(),
                                    new BasicArguments(ImmutableMap.of(
                                      ProgramOptionConstants.INSTANCE_ID, "0",
                                      ProgramOptionConstants.PRINCIPAL, "principal",
                                      ProgramOptionConstants.RUN_ID, programRunId.getRun(),
                                      ProgramOptionConstants.CLUSTER_MODE, clusterMode.name())),
                                    new BasicArguments());
  }
}
