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

package io.cdap.cdap.internal.app.program;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ProgramStateWriterWithHeartBeat}.
 */
public class ProgramStateWriterWithHeartBeatTest {
  private static class MockProgramStatePublisher implements ProgramStatePublisher {
    long heartBeatCount;

    @Override
    public void publish(Notification.Type notificationType, Map<String, String> properties) {
      if (Notification.Type.PROGRAM_HEART_BEAT.equals(notificationType)) {
        Assert.assertTrue(properties.containsKey(ProgramOptionConstants.HEART_BEAT_TIME));
        heartBeatCount++;
      }
    }

    long getHeartBeatCount() {
      return heartBeatCount;
    }
  }

  @Test
  public void testProgramCompleted() throws Exception {
    testHeartBeatThread(ProgramRunStatus.COMPLETED);
  }

  @Test
  public void testProgramKilled() throws Exception {
    testHeartBeatThread(ProgramRunStatus.KILLED);
  }

  @Test
  public void testProgramFailed() throws Exception {
    testHeartBeatThread(ProgramRunStatus.FAILED);
  }

  private void testHeartBeatThread(ProgramRunStatus terminalState) throws Exception {
    // configure program state writer to emit heart beat every second
    MockProgramStatePublisher programStatePublisher = new MockProgramStatePublisher();

    AtomicReference<ProgramRunStatus> completionState = new AtomicReference<>();
    ProgramStateWriter programStateWriter = new NoOpProgramStateWriter() {
      @Override
      public void completed(ProgramRunId programRunId) {
        super.completed(programRunId);
        completionState.set(ProgramRunStatus.COMPLETED);
      }

      @Override
      public void killed(ProgramRunId programRunId) {
        super.killed(programRunId);
        completionState.set(ProgramRunStatus.KILLED);
      }

      @Override
      public void error(ProgramRunId programRunId, Throwable failureCause) {
        super.error(programRunId, failureCause);
        completionState.set(ProgramRunStatus.FAILED);
      }
    };

    // mock program configurations
    ProgramId programId = NamespaceId.DEFAULT.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());
    ProgramRunId runId = programId.run(RunIds.generate());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    ProgramStateWriterWithHeartBeat programStateWriterWithHeartBeat =
      new ProgramStateWriterWithHeartBeat(runId, programStateWriter, 1, programStatePublisher);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", ProjectInfo.getVersion().toString(), "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    // start the program and ensure heart beat is 0 before we call running
    programStateWriter.start(runId, programOptions, null, programDescriptor);
    Assert.assertEquals(0, programStatePublisher.getHeartBeatCount());
    programStateWriterWithHeartBeat.running(null);

    // on running, we start receiving heart beat messages, verify if the heartbeat count goes to 2.
    Tasks.waitFor(true , () -> programStatePublisher.getHeartBeatCount() > 1,
                  10, TimeUnit.SECONDS, "Didn't receive expected heartbeat after 10 seconds");

    // Terminate the program and make sure the heart beat thread also gets stopped
    switch (terminalState) {
      case COMPLETED:
        programStateWriterWithHeartBeat.completed();
        break;
      case FAILED:
        programStateWriterWithHeartBeat.error(new RuntimeException());
        break;
      case KILLED:
        programStateWriterWithHeartBeat.killed();
        break;
      default:
        throw new IllegalStateException("The terminal state must one of COMPLETED, FAILED, or KILLED");
    }

    Tasks.waitFor(false , programStateWriterWithHeartBeat::isHeartBeatThreadAlive,
                  5, TimeUnit.SECONDS, "Heartbeat thread did not stop after 5 seconds");

    Assert.assertEquals(terminalState, completionState.get());
  }
}
