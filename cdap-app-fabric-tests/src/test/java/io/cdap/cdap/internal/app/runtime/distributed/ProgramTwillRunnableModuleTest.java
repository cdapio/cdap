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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.runtime.spark.SparkProgramRunner;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import io.cdap.cdap.app.runtime.spark.distributed.SparkTwillRunnable;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.test.MockTwillContext;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.service.ServiceProgramRunner;
import io.cdap.cdap.internal.app.runtime.worker.WorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramRunner;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ServiceAnnouncer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for guice modules used in various {@link AbstractProgramTwillRunnable}.
 */
@RunWith(Parameterized.class)
public class ProgramTwillRunnableModuleTest {

  // There are three variables that we need to test all combinations of them
  // With or without MasterEnvironment
  // ISOLATED vs ON_PREMISE ClusterMode
  // With or without peerName program options (for tethering)

  // Runs two sets of tests, one with master environment, one without
  @Parameterized.Parameters(name = "Master environment = {0}, Cluster mode = {1}, Peer name = {2}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {true, ClusterMode.ON_PREMISE, null},
      {true, ClusterMode.ON_PREMISE, "peer"},
      {true, ClusterMode.ISOLATED, null},
      {true, ClusterMode.ISOLATED, "peer"},
      {false, ClusterMode.ON_PREMISE, null},
      {false, ClusterMode.ON_PREMISE, "peer"},
      {false, ClusterMode.ISOLATED, null},
      {false, ClusterMode.ISOLATED, "peer"},
    });
  }

  private final ClusterMode clusterMode;
  private final String peerName;

  public ProgramTwillRunnableModuleTest(boolean useMasterEnv, ClusterMode clusterMode, @Nullable  String peerName) {
    if (useMasterEnv) {
      MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    } else {
      MasterEnvironments.setMasterEnvironment(null);
    }

    this.clusterMode = clusterMode;
    this.peerName = peerName;
  }

  @Test
  public void testService() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").service("service").run(RunIds.generate());
    Module module = new ServiceTwillRunnable("service") {
      @Override
      protected ServiceAnnouncer getServiceAnnouncer() {
        return new MockTwillContext();
      }
    }.createModule(CConfiguration.create(), new Configuration(),
                   createProgramOptions(programRunId), programRunId);
    Injector injector = Guice.createInjector(module, new NoOpAuditLogModule());
    injector.getInstance(ServiceProgramRunner.class);
    injector.getInstance(ProgramStateWriter.class);
  }

  @Test
  public void testWorker() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").worker("worker").run(RunIds.generate());
    Module module = new WorkerTwillRunnable("worker").createModule(CConfiguration.create(), new Configuration(),
                                                                   createProgramOptions(programRunId),
                                                                   programRunId);
    Injector injector = Guice.createInjector(module);
    injector.getInstance(WorkerProgramRunner.class);
    injector.getInstance(ProgramStateWriter.class);
  }

  @Test
  public void testMapReduce() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").mr("mapreduce").run(RunIds.generate());
    Module module = new MapReduceTwillRunnable("mapreduce").createModule(CConfiguration.create(), new Configuration(),
                                                                         createProgramOptions(programRunId),
                                                                         programRunId);
    Injector injector = Guice.createInjector(module);
    injector.getInstance(MapReduceProgramRunner.class);
    injector.getInstance(ProgramStateWriter.class);
  }

  @Test
  public void testWorkflow() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").workflow("workflow").run(RunIds.generate());
    Module module = new WorkflowTwillRunnable("workflow").createModule(CConfiguration.create(), new Configuration(),
                                                                       createProgramOptions(programRunId),
                                                                       programRunId);
    Injector injector = Guice.createInjector(module);
    injector.getInstance(WorkflowProgramRunner.class);
    // Workflow supports spark, which supports PluginFinder
    injector.getInstance(PluginFinder.class);
    injector.getInstance(ProgramStateWriter.class);
  }

  @Test
  public void testSpark() {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("test").spark("spark").run(RunIds.generate());

    Module module = new SparkTwillRunnable("spark") {
      @Override
      protected ServiceAnnouncer getServiceAnnouncer() {
        return new MockTwillContext();
      }
    }.createModule(CConfiguration.create(), new Configuration(),
                   createProgramOptions(programRunId), programRunId);

    Injector injector = Guice.createInjector(module,new NoOpAuditLogModule());
    injector.getInstance(SparkProgramRunner.class);
    injector.getInstance(ProgramStateWriter.class);

    Injector contextInjector = SparkRuntimeContextProvider.createInjector(CConfiguration.create(),
                                                                          new Configuration(),
                                                                          programRunId.getParent(),
                                                                          createProgramOptions(programRunId));
    contextInjector.getInstance(PluginFinder.class);
    contextInjector.getInstance(ProgramStateWriter.class);
  }

  private ProgramOptions createProgramOptions(ProgramRunId programRunId) {
    Map<String, String> systemArgs = new HashMap<>();

    systemArgs.put(ProgramOptionConstants.INSTANCE_ID, "0");
    systemArgs.put(ProgramOptionConstants.PRINCIPAL, "principal");
    systemArgs.put(ProgramOptionConstants.RUN_ID, programRunId.getRun());
    systemArgs.put(ProgramOptionConstants.CLUSTER_MODE, clusterMode.name());

    if (peerName != null) {
      systemArgs.put(ProgramOptionConstants.PEER_NAME, peerName);
    }

    return new SimpleProgramOptions(programRunId.getParent(), new BasicArguments(systemArgs), new BasicArguments());
  }
}
