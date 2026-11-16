/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowConditionNode;
import io.cdap.cdap.api.workflow.WorkflowConfigurer;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowForkNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.ProgramClassLoaderProvider;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link WorkflowDriver}.
 */
public class WorkflowDriverTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final AssertionError FATAL_BRANCH_ERROR =
      new AssertionError("fork branch fatal error");

  /**
   * Minimal workflow implementation without {@link io.cdap.cdap.api.ProgramLifecycle}.
   */
  public static final class DummyWorkflow implements Workflow {
    @Override
    public void configure(WorkflowConfigurer configurer) {
    }
  }

  /**
   * Predicate throwing {@link AssertionError} directly on fork worker thread.
   */
  public static final class FailingPredicate implements Predicate<WorkflowContext> {
    @Override
    public boolean apply(WorkflowContext input) {
      throw FATAL_BRANCH_ERROR;
    }
  }

  @Test
  public void testSuccessAndPluginInstantiatorClosedOnShutdown() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowSpecification wfSpec = createWorkflowSpec(Collections.emptyList());
    Program program = createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());

    AtomicBoolean pluginClosed = new AtomicBoolean(false);
    PluginInstantiator pluginInstantiator = new PluginInstantiator(
        cConf, program.getClassLoader(), TEMP_FOLDER.newFolder()) {
      @Override
      public void close() throws IOException {
        pluginClosed.set(true);
        super.close();
      }
    };

    WorkflowDriver driver = createDriver(
        cConf, program, wfSpec, programType -> null, pluginInstantiator);

    CompletableFuture<Service.State> done = attachTerminalListener(driver);
    driver.start();
    Assert.assertNotNull(done.get(5, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.TERMINATED, driver.state());
    Assert.assertTrue(pluginClosed.get());
    program.close();
  }

  @Test
  public void testExecuteActionFailurePropagatesAndTransitionsToFailed() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowActionNode actionNode = new WorkflowActionNode(
        "mrAction", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "mr1"));
    WorkflowSpecification wfSpec = createWorkflowSpec(Collections.singletonList(actionNode));
    Program program = createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());

    IllegalStateException actionFailure = new IllegalStateException("action runner boom");
    ProgramRunnerFactory failingFactory = programType -> {
      throw actionFailure;
    };

    WorkflowDriver driver = createDriver(cConf, program, wfSpec, failingFactory, null);
    CompletableFuture<Service.State> done = attachTerminalListener(driver);
    driver.start();

    try {
      done.get(5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException when action fails");
    } catch (ExecutionException expected) {
      Assert.assertTrue(expected.getCause() instanceof ExecutionException);
      Assert.assertSame(actionFailure, expected.getCause().getCause());
    }
    Assert.assertEquals(Service.State.FAILED, driver.state());
    program.close();
  }

  @Test
  public void testExecuteForkUnwrapsExecutionExceptionAndPropagatesError() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowConditionNode branchCondition = new WorkflowConditionNode(
        "forkCond",
        FailingPredicate.class.getName(),
        Collections.emptyList(),
        Collections.emptyList());
    WorkflowForkNode forkNode = new WorkflowForkNode(
        "fork1", ImmutableList.of(Collections.singletonList(branchCondition)));
    WorkflowSpecification wfSpec = createWorkflowSpec(Collections.singletonList(forkNode));
    Program program = createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());

    WorkflowDriver driver = createDriver(cConf, program, wfSpec, programType -> null, null);
    CompletableFuture<Service.State> done = attachTerminalListener(driver);
    driver.start();

    try {
      done.get(5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException from failed fork");
    } catch (ExecutionException expected) {
      Assert.assertSame(FATAL_BRANCH_ERROR, expected.getCause());
    }
    Assert.assertEquals(Service.State.FAILED, driver.state());
    program.close();
  }

  static WorkflowSpecification createWorkflowSpec(List<WorkflowNode> nodes) {
    return new WorkflowSpecification(
        DummyWorkflow.class.getName(), "wf", "desc",
        Collections.emptyMap(), nodes, Collections.emptyMap(), Collections.emptyMap());
  }

  static Program createProgram(CConfiguration cConf, WorkflowSpecification wfSpec, File workDir)
      throws IOException {
    File jarFile = new File(workDir, "program.jar");
    Assert.assertTrue(jarFile.createNewFile());
    File unpackedDir = new File(workDir, "unpacked");
    Assert.assertTrue(unpackedDir.mkdirs());

    MapReduceSpecification mrSpec = new MapReduceSpecification(
        "io.cdap.cdap.DummyMr", "mr1", "desc", null, null,
        Collections.emptySet(), Collections.emptyMap(), null, null, null, Collections.emptyMap());
    ArtifactId artifactId = new ArtifactId("art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "wfApp", "1.0.0", "6.0.0", "desc", null, artifactId,
        Collections.emptyMap(), Collections.emptyMap(),
        ImmutableMap.of("mr1", mrSpec), Collections.emptyMap(),
        ImmutableMap.of("wf", wfSpec), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ProgramId wfId = NamespaceId.DEFAULT.app("wfApp").workflow("wf");
    ProgramRunner classLoaderRunner = new TestClassLoaderProgramRunner();
    return Programs.create(
        cConf,
        classLoaderRunner,
        new ProgramDescriptor(wfId, appSpec),
        Locations.toLocation(jarFile),
        unpackedDir);
  }

  static ProgramOptions createProgramOptions(ProgramId programId) {
    return new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(
            ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
            ProgramOptionConstants.ARTIFACT_ID, "default:art:1.0.0")),
        new BasicArguments());
  }

  static WorkflowDriver createDriver(
      CConfiguration cConf,
      Program program,
      WorkflowSpecification wfSpec,
      ProgramRunnerFactory runnerFactory,
      @Nullable PluginInstantiator pluginInstantiator) {
    ProgramOptions options = createProgramOptions(program.getId());
    return new WorkflowDriver(
        program,
        options,
        wfSpec,
        runnerFactory,
        new NoOpMetricsCollectionService(),
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(DiscoveryServiceClient.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(WorkflowStateWriter.class),
        cConf,
        pluginInstantiator,
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(MessagingService.class),
        Mockito.mock(ProgramStateWriter.class),
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(AppStateStoreProvider.class));
  }

  private static final class TestClassLoaderProgramRunner
      implements ProgramRunner, ProgramClassLoaderProvider {
    @Override
    public ProgramController run(Program program, ProgramOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClassLoader createProgramClassLoaderParent() {
      return WorkflowDriverTest.class.getClassLoader();
    }
  }

  private static CompletableFuture<Service.State> attachTerminalListener(Service service) {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return future;
  }
}
