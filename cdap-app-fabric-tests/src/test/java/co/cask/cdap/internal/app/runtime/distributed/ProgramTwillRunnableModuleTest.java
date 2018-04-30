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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.test.MockTwillContext;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.flow.FlowletProgramRunner;
import co.cask.cdap.internal.app.runtime.service.ServiceProgramRunner;
import co.cask.cdap.internal.app.runtime.worker.WorkerProgramRunner;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Tests for guice modules used in various {@link AbstractProgramTwillRunnable}.
 */
public class ProgramTwillRunnableModuleTest {

  @Test
  public void testFlowlet() {
    ProgramId programId = NamespaceId.DEFAULT.app("test").flow("flow");
    Module module = new FlowletTwillRunnable("flowlet").createModule(CConfiguration.create(), new Configuration(),
                                                                     new MockTwillContext(), programId,
                                                                     RunIds.generate().getId(), "0", "principal");
    Guice.createInjector(module).getInstance(FlowletProgramRunner.class);
  }

  @Test
  public void testService() {
    ProgramId programId = NamespaceId.DEFAULT.app("test").service("service");
    Module module = new ServiceTwillRunnable("service").createModule(CConfiguration.create(), new Configuration(),
                                                                     new MockTwillContext(), programId,
                                                                     RunIds.generate().getId(), "0", "principal");
    Guice.createInjector(module).getInstance(ServiceProgramRunner.class);
  }

  @Test
  public void testWorker() {
    ProgramId programId = NamespaceId.DEFAULT.app("test").worker("worker");
    Module module = new WorkerTwillRunnable("worker").createModule(CConfiguration.create(), new Configuration(),
                                                                   new MockTwillContext(), programId,
                                                                   RunIds.generate().getId(), "0", "principal");
    Guice.createInjector(module).getInstance(WorkerProgramRunner.class);
  }

  @Test
  public void testMapReduce() {
    ProgramId programId = NamespaceId.DEFAULT.app("test").mr("mapreduce");
    Module module = new MapReduceTwillRunnable("mapreduce").createModule(CConfiguration.create(), new Configuration(),
                                                                         new MockTwillContext(), programId,
                                                                         RunIds.generate().getId(), "0", "principal");
    Guice.createInjector(module).getInstance(MapReduceProgramRunner.class);
  }

  @Test
  public void testWorkflow() {
    ProgramId programId = NamespaceId.DEFAULT.app("test").workflow("workflow");
    Module module = new WorkflowTwillRunnable("workflow").createModule(CConfiguration.create(), new Configuration(),
                                                                       new MockTwillContext(), programId,
                                                                       RunIds.generate().getId(), "0", "principal");
    Injector injector = Guice.createInjector(module);
    injector.getInstance(WorkflowProgramRunner.class);
    // Workflow supports spark, which supports PluginFinder
    injector.getInstance(PluginFinder.class);
  }
}
