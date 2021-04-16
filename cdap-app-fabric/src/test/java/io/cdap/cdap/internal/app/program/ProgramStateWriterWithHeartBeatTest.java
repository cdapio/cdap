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
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProgramStateWriterWithHeartBeatTest {
  private static class MockProgramStatePublisher implements ProgramStatePublisher {
    long heartBeatCount = 0;

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
  public void testHeartBeatThread() throws InterruptedException, ExecutionException, TimeoutException {
    // configure program state writer to emit heart beat every second
    ProgramStatePublisher programStatePublisher = new MockProgramStatePublisher();
    NoOpProgramStateWriter programStateWriter = new NoOpProgramStateWriter();

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
    programStateWriterWithHeartBeat.start(programOptions, null, programDescriptor);
    Assert.assertEquals(0, ((MockProgramStatePublisher) programStatePublisher).getHeartBeatCount());
    programStateWriterWithHeartBeat.running(null);

    // on running, we start receiving heart beat messages, verify if we heart beat count goes to 2.
    Tasks.waitFor(true , () -> ((MockProgramStatePublisher) programStatePublisher).getHeartBeatCount() > 1,
                  10, TimeUnit.SECONDS, "Didn't receive expected heartbeat after 10 seconds");

    // make sure suspending program suspended the heartbeat thread
    programStateWriterWithHeartBeat.suspend();
    Tasks.waitFor(false , () -> programStateWriterWithHeartBeat.isHeartBeatThreadAlive(),
                  5, TimeUnit.SECONDS, "Heartbeat thread did not stop after 5 seconds");
    long heartBeatAfterSuspend = ((MockProgramStatePublisher) programStatePublisher).getHeartBeatCount();

    // resume the program and make sure that the heart beat messages goes up after resuming program
    programStateWriterWithHeartBeat.resume();
    long expected = heartBeatAfterSuspend + 1;
    Tasks.waitFor(true , () -> ((MockProgramStatePublisher) programStatePublisher).getHeartBeatCount() > expected,
                  10, TimeUnit.SECONDS, "Didn't receive expected heartbeat after 10 seconds after resuming program");

    // kill the program and make sure the heart beat thread also gets stopped
    programStateWriterWithHeartBeat.killed();
    Tasks.waitFor(false , () -> programStateWriterWithHeartBeat.isHeartBeatThreadAlive(),
                  5, TimeUnit.SECONDS, "Heartbeat thread did not stop after 5 seconds");
  }
}
