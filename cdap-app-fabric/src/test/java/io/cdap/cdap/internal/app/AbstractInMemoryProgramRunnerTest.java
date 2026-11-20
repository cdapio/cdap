/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app;

import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractInMemoryProgramRunner}.
 */
public class AbstractInMemoryProgramRunnerTest {

  private static final ProgramId PROGRAM_ID =
      NamespaceId.DEFAULT.app("app").program(ProgramType.WORKER, "worker");

  private Program createMockProgram() {
    Program program = Mockito.mock(Program.class);
    Mockito.when(program.getId()).thenReturn(PROGRAM_ID);
    Mockito.when(program.getName()).thenReturn(PROGRAM_ID.getProgram());
    return program;
  }

  private ProgramOptions createOptions() {
    return new SimpleProgramOptions(
        PROGRAM_ID,
        new BasicArguments(Collections.singletonMap(
            ProgramOptionConstants.RUN_ID, RunIds.generate().getId())),
        new BasicArguments(Collections.emptyMap()));
  }

  @Test
  public void testStartAllAndStopAllInstances() throws Exception {
    List<TestComponentController> createdControllers =
        Collections.synchronizedList(new ArrayList<>());

    AbstractInMemoryProgramRunner runner =
        new AbstractInMemoryProgramRunner(CConfiguration.create()) {
          @Override
          protected ProgramRunner createProgramRunner() {
            return (program, options) -> {
              TestComponentController controller = new TestComponentController(options);
              createdControllers.add(controller);
              return controller;
            };
          }

          @Override
          public ProgramController run(Program program, ProgramOptions options) {
            return startAll(program, options, 3);
          }
        };

    ProgramController compositeController = runner.run(createMockProgram(), createOptions());
    Assert.assertEquals(3, createdControllers.size());
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(
          Integer.toString(i),
          createdControllers.get(i).options.getArguments()
              .getOption(ProgramOptionConstants.INSTANCE_ID));
      Assert.assertEquals(
          "3",
          createdControllers.get(i).options.getArguments()
              .getOption(ProgramOptionConstants.INSTANCES));
    }

    compositeController.stop().get(5, TimeUnit.SECONDS);
    for (TestComponentController child : createdControllers) {
      Tasks.waitFor(ProgramController.State.KILLED, child::getState,
          5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  public void testStartupFailureStopsStartedInstancesAndPropagatesRuntimeExceptionUnwrapped()
      throws Exception {
    List<TestComponentController> startedBeforeFailure =
        Collections.synchronizedList(new ArrayList<>());
    IllegalStateException expected = new IllegalStateException("third instance failed");
    AtomicInteger count = new AtomicInteger();

    AbstractInMemoryProgramRunner runner =
        new AbstractInMemoryProgramRunner(CConfiguration.create()) {
          @Override
          protected ProgramRunner createProgramRunner() {
            return (program, options) -> {
              if (count.getAndIncrement() == 2) {
                throw expected;
              }
              TestComponentController controller = new TestComponentController(options);
              startedBeforeFailure.add(controller);
              return controller;
            };
          }

          @Override
          public ProgramController run(Program program, ProgramOptions options) {
            return startAll(program, options, 3);
          }
        };

    try {
      runner.run(createMockProgram(), createOptions());
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }

    Assert.assertEquals(2, startedBeforeFailure.size());
    for (TestComponentController child : startedBeforeFailure) {
      Tasks.waitFor(ProgramController.State.KILLED, child::getState,
          5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  public void testStartupFailurePropagatesErrorUnwrapped() throws Exception {
    List<TestComponentController> startedBeforeFailure =
        Collections.synchronizedList(new ArrayList<>());
    LinkageError expectedError = new LinkageError("missing native symbol");
    AtomicInteger count = new AtomicInteger();

    AbstractInMemoryProgramRunner runner =
        new AbstractInMemoryProgramRunner(CConfiguration.create()) {
          @Override
          protected ProgramRunner createProgramRunner() {
            return (program, options) -> {
              if (count.getAndIncrement() == 1) {
                throw expectedError;
              }
              TestComponentController controller = new TestComponentController(options);
              startedBeforeFailure.add(controller);
              return controller;
            };
          }

          @Override
          public ProgramController run(Program program, ProgramOptions options) {
            return startAll(program, options, 2);
          }
        };

    try {
      runner.run(createMockProgram(), createOptions());
      Assert.fail("Expected LinkageError");
    } catch (LinkageError actual) {
      Assert.assertSame(expectedError, actual);
    }

    Assert.assertEquals(1, startedBeforeFailure.size());
    Tasks.waitFor(ProgramController.State.KILLED, startedBeforeFailure.get(0)::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
  }

  private static final class TestComponentController extends AbstractProgramController {

    private final ProgramOptions options;

    private TestComponentController(ProgramOptions options) {
      super(options.getProgramId().run(RunIds.generate()));
      this.options = options;
      started();
    }

    @Override
    protected void doSuspend() {
      // no-op
    }

    @Override
    protected void doResume() {
      // no-op
    }

    @Override
    protected void doStop() {
      // no-op
    }

    @Override
    protected void doCommand(String name, Object value) {
      // no-op
    }
  }
}
