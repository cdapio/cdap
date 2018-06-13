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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * ProgramLifecycleService tests.
 */
public class ProgramLifecycleServiceTest {

  @Test
  public void testEmptyRunsIsStopped() {
    Assert.assertEquals(ProgramStatus.STOPPED, ProgramLifecycleService.getProgramStatus(Collections.emptyList()));
  }

  @Test
  public void testProgramStatusFromSingleRun() {
    RunRecordMeta record = RunRecordMeta.builder()
      .setProgramRunId(NamespaceId.DEFAULT.app("app").mr("mr").run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(new ArtifactId("r", new ArtifactVersion("1.0"), ArtifactScope.USER))
      .setStatus(ProgramRunStatus.PENDING)
      .setSourceId(new byte[] { 0 })
      .build();

    // pending or starting -> starting
    ProgramStatus status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.STARTING).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // running, suspended, resuming -> running
    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.RUNNING).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.SUSPENDED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // failed, killed, completed -> stopped
    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.FAILED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.KILLED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordMeta.builder(record).setStatus(ProgramRunStatus.COMPLETED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);
  }

  @Test
  public void testProgramStatusFromMultipleRuns() {
    ProgramId programId = NamespaceId.DEFAULT.app("app").mr("mr");
    RunRecordMeta pending = RunRecordMeta.builder()
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(new ArtifactId("r", new ArtifactVersion("1.0"), ArtifactScope.USER))
      .setStatus(ProgramRunStatus.PENDING)
      .setSourceId(new byte[] { 0 })
      .build();
    RunRecordMeta starting = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.STARTING).build();
    RunRecordMeta running = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.RUNNING).build();
    RunRecordMeta killed = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.KILLED).build();
    RunRecordMeta failed = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.FAILED).build();
    RunRecordMeta completed = RunRecordMeta.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.COMPLETED).build();

    // running takes precedence over others
    ProgramStatus status = ProgramLifecycleService.getProgramStatus(
      Arrays.asList(pending, starting, running, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // starting takes precedence over stopped
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(pending, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(starting, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // end states are stopped
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STOPPED, status);
  }
}
