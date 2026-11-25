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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.runtime.WorkflowDataProvider;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultProgramWorkflowRunner}.
 */
public class DefaultProgramWorkflowRunnerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCreateFailureClosesProgramRunnerAndPropagatesUnchecked() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Program workflowProgram = createWorkflowProgram(cConf);
    ProgramOptions workflowOptions = createWorkflowOptions(workflowProgram.getId());
    WorkflowSpecification workflowSpec = workflowProgram.getApplicationSpecification()
        .getWorkflows().get("wf");
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    Map<String, WorkflowNodeState> nodeStates = new HashMap<>();
    AtomicInteger runnerCloseCount = new AtomicInteger();

    CloseableProgramRunner closeableRunner = new CloseableProgramRunner(runnerCloseCount) {
      @Override
      public ProgramController run(Program program, ProgramOptions options) {
        throw new UnsupportedOperationException();
      }
    };
    ProgramRunnerFactory runnerFactory = programType -> closeableRunner;

    DefaultProgramWorkflowRunner workflowRunner = new DefaultProgramWorkflowRunner(
        cConf, workflowProgram, workflowOptions, runnerFactory, workflowSpec,
        token, "node1", nodeStates, ProgramType.MAPREDUCE, Mockito.mock(ProgramStateWriter.class));

    try {
      // "nonExistentMr" is missing from appSpec, causing Programs.create to throw IllegalArgumentException
      workflowRunner.create("nonExistentMr");
      Assert.fail("Expected IllegalArgumentException for missing MapReduce program");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("nonExistentMr"));
    }
    Assert.assertEquals(1, runnerCloseCount.get());
    workflowProgram.close();
  }

  @Test
  public void testRunStartupFailureClosesResourcesAndRecordsError() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Program workflowProgram = createWorkflowProgram(cConf);
    ProgramOptions workflowOptions = createWorkflowOptions(workflowProgram.getId());
    WorkflowSpecification workflowSpec = workflowProgram.getApplicationSpecification()
        .getWorkflows().get("wf");
    BasicWorkflowToken token = new BasicWorkflowToken(1);
    Map<String, WorkflowNodeState> nodeStates = new HashMap<>();
    AtomicInteger runnerCloseCount = new AtomicInteger();
    ProgramStateWriter stateWriter = Mockito.mock(ProgramStateWriter.class);
    IllegalStateException startFailure = new IllegalStateException("runner failed to start");

    CloseableProgramRunner failingRunner = new CloseableProgramRunner(runnerCloseCount) {
      @Override
      public ProgramController run(Program program, ProgramOptions options) {
        throw startFailure;
      }
    };

    DefaultProgramWorkflowRunner workflowRunner = new DefaultProgramWorkflowRunner(
        cConf, workflowProgram, workflowOptions, programType -> failingRunner, workflowSpec,
        token, "mrNode", nodeStates, ProgramType.MAPREDUCE, stateWriter);

    Runnable runnable = workflowRunner.create("mr1");
    try {
      runnable.run();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException actual) {
      Assert.assertSame(startFailure, actual);
    }

    Assert.assertEquals(1, runnerCloseCount.get());
    Mockito.verify(stateWriter).start(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verify(stateWriter).error(Mockito.any(), Mockito.same(startFailure));
    workflowProgram.close();
  }

  @Test
  public void testRunCompletedKilledAndErrorCompletionStates() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Program workflowProgram = createWorkflowProgram(cConf);
    ProgramOptions workflowOptions = createWorkflowOptions(workflowProgram.getId());
    WorkflowSpecification workflowSpec = workflowProgram.getApplicationSpecification()
        .getWorkflows().get("wf");

    // 1. Completed execution updates token, node state, and closes runner quietly
    BasicWorkflowToken completedToken = new BasicWorkflowToken(1);
    Map<String, WorkflowNodeState> completedStates = new HashMap<>();
    AtomicInteger completedCloseCount = new AtomicInteger();
    CloseableProgramRunner completingRunner = new CloseableProgramRunner(completedCloseCount) {
      @Override
      public ProgramController run(Program program, ProgramOptions options) {
        BasicWorkflowToken childToken = new BasicWorkflowToken(1);
        childToken.setCurrentNode("mrNode");
        childToken.put("k", "v");
        FakeWorkflowController controller = new FakeWorkflowController(
            program.getId().run(ProgramRunners.getRunId(options)), childToken);
        controller.completeNow();
        return controller;
      }
    };
    DefaultProgramWorkflowRunner runner1 = new DefaultProgramWorkflowRunner(
        cConf, workflowProgram, workflowOptions, type -> completingRunner, workflowSpec,
        completedToken, "mrNode", completedStates, ProgramType.MAPREDUCE,
        Mockito.mock(ProgramStateWriter.class));
    runner1.create("mr1").run();
    Assert.assertEquals(1, completedCloseCount.get());
    Assert.assertEquals(NodeStatus.COMPLETED, completedStates.get("mrNode").getNodeStatus());
    Assert.assertEquals("v", completedToken.get("k", "mrNode").toString());

    // 2. Killed execution records KILLED node state and closes runner quietly
    BasicWorkflowToken killedToken = new BasicWorkflowToken(1);
    Map<String, WorkflowNodeState> killedStates = new HashMap<>();
    AtomicInteger killedCloseCount = new AtomicInteger();
    CloseableProgramRunner killedRunner = new CloseableProgramRunner(killedCloseCount) {
      @Override
      public ProgramController run(Program program, ProgramOptions options) {
        FakeWorkflowController controller = new FakeWorkflowController(
            program.getId().run(ProgramRunners.getRunId(options)), new BasicWorkflowToken(1));
        controller.killNow();
        return controller;
      }
    };
    DefaultProgramWorkflowRunner runner2 = new DefaultProgramWorkflowRunner(
        cConf, workflowProgram, workflowOptions, type -> killedRunner, workflowSpec,
        killedToken, "mrNode", killedStates, ProgramType.MAPREDUCE,
        Mockito.mock(ProgramStateWriter.class));
    runner2.create("mr1").run();
    Assert.assertEquals(1, killedCloseCount.get());
    Assert.assertEquals(NodeStatus.KILLED, killedStates.get("mrNode").getNodeStatus());

    // 3. Error execution with checked IOException wraps via Throwables.propagate and closes runner
    BasicWorkflowToken errorToken = new BasicWorkflowToken(1);
    Map<String, WorkflowNodeState> errorStates = new HashMap<>();
    AtomicInteger errorCloseCount = new AtomicInteger();
    IOException ioFailure = new IOException("mapreduce failed");
    CloseableProgramRunner errorRunner = new CloseableProgramRunner(errorCloseCount) {
      @Override
      public ProgramController run(Program program, ProgramOptions options) {
        FakeWorkflowController controller = new FakeWorkflowController(
            program.getId().run(ProgramRunners.getRunId(options)), new BasicWorkflowToken(1));
        controller.failNow(ioFailure);
        return controller;
      }
    };
    DefaultProgramWorkflowRunner runner3 = new DefaultProgramWorkflowRunner(
        cConf, workflowProgram, workflowOptions, type -> errorRunner, workflowSpec,
        errorToken, "mrNode", errorStates, ProgramType.MAPREDUCE,
        Mockito.mock(ProgramStateWriter.class));
    try {
      runner3.create("mr1").run();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException expected) {
      Assert.assertSame(ioFailure, expected.getCause());
    }
    Assert.assertEquals(1, errorCloseCount.get());
    Assert.assertEquals(NodeStatus.FAILED, errorStates.get("mrNode").getNodeStatus());

    // 4. Error execution with Error propagates Error unwrapped
    AssertionError fatalError = new AssertionError("fatal child error");
    AtomicInteger fatalCloseCount = new AtomicInteger();
    CloseableProgramRunner fatalRunner = new CloseableProgramRunner(fatalCloseCount) {
      @Override
      public ProgramController run(Program program, ProgramOptions options) {
        FakeWorkflowController controller = new FakeWorkflowController(
            program.getId().run(ProgramRunners.getRunId(options)), new BasicWorkflowToken(1));
        controller.failNow(fatalError);
        return controller;
      }
    };
    DefaultProgramWorkflowRunner runner4 = new DefaultProgramWorkflowRunner(
        cConf, workflowProgram, workflowOptions, type -> fatalRunner, workflowSpec,
        new BasicWorkflowToken(1), "mrNode", new HashMap<>(), ProgramType.MAPREDUCE,
        Mockito.mock(ProgramStateWriter.class));
    try {
      runner4.create("mr1").run();
      Assert.fail("Expected AssertionError");
    } catch (AssertionError actual) {
      Assert.assertSame(fatalError, actual);
    }
    Assert.assertEquals(1, fatalCloseCount.get());
    workflowProgram.close();
  }

  private static Program createWorkflowProgram(CConfiguration cConf) throws IOException {
    File jarFile = TEMP_FOLDER.newFile();
    File unpackedDir = TEMP_FOLDER.newFolder();
    MapReduceSpecification mrSpec = new MapReduceSpecification(
        "io.cdap.cdap.DummyMr", "mr1", "desc", null, null,
        Collections.emptySet(), Collections.emptyMap(), null, null, null, Collections.emptyMap());
    WorkflowSpecification wfSpec = new WorkflowSpecification(
        "io.cdap.cdap.DummyWorkflow", "wf", "desc",
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
    ArtifactId artifactId = new ArtifactId("art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "wfApp", "1.0.0", "6.0.0", "desc", null, artifactId,
        Collections.emptyMap(), Collections.emptyMap(),
        ImmutableMap.of("mr1", mrSpec), Collections.emptyMap(),
        ImmutableMap.of("wf", wfSpec), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ProgramId wfId = NamespaceId.DEFAULT.app("wfApp").workflow("wf");
    return Programs.create(
        cConf, null, new ProgramDescriptor(wfId, appSpec), Locations.toLocation(jarFile), unpackedDir);
  }

  private static ProgramOptions createWorkflowOptions(ProgramId wfId) {
    Map<String, String> sysArgs = ImmutableMap.of(
        ProgramOptionConstants.RUN_ID, RunIds.generate().getId());
    return new SimpleProgramOptions(wfId, new BasicArguments(sysArgs), new BasicArguments());
  }

  private abstract static class CloseableProgramRunner implements ProgramRunner, Closeable {
    private final AtomicInteger closeCount;

    private CloseableProgramRunner(AtomicInteger closeCount) {
      this.closeCount = closeCount;
    }

    @Override
    public void close() throws IOException {
      closeCount.incrementAndGet();
      throw new IOException("ignored close exception");
    }
  }

  private static final class FakeWorkflowController extends AbstractProgramController
      implements WorkflowDataProvider {
    private final WorkflowToken workflowToken;

    private FakeWorkflowController(ProgramRunId programRunId, WorkflowToken workflowToken) {
      super(programRunId);
      this.workflowToken = workflowToken;
      started();
    }

    void completeNow() {
      complete();
    }

    void killNow() {
      stop();
    }

    void failNow(Throwable cause) {
      error(cause);
    }

    @Override
    protected void doSuspend() {
    }

    @Override
    protected void doResume() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doCommand(String name, Object value) {
    }

    @Override
    public WorkflowToken getWorkflowToken() {
      return workflowToken;
    }

    @Override
    public Set<Operation> getFieldLineageOperations() {
      return Collections.emptySet();
    }
  }
}
