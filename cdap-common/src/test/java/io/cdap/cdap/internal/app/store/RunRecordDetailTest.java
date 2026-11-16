/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RunRecordDetail}.
 */
public class RunRecordDetailTest {

  @Test
  public void testBuilderEqualsAndExactToString() {
    ProgramRunId runId = new ProgramRunId(
        "default", "myApp", ProgramType.WORKFLOW, "myWorkflow", "run-123");
    ArtifactId artifactId = new ArtifactId(
        "my-artifact", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ProgramRunCluster cluster = new ProgramRunCluster(
        ProgramRunClusterStatus.PROVISIONED, 200L, 4);
    byte[] sourceId = new byte[]{0x0a, (byte) 0xff};

    RunRecordDetail detail = RunRecordDetail.builder()
        .setProgramRunId(runId)
        .setStartTime(100L)
        .setRunTime(110L)
        .setStopTime(210L)
        .setSuspendTime(130L)
        .setResumeTime(140L)
        .setStoppingTime(190L)
        .setTerminateTs(220L)
        .setStatus(ProgramRunStatus.COMPLETED)
        .setProperties(Collections.singletonMap("runtimeArgs", "{\"k\":\"v\"}"))
        .setSystemArgs(Collections.singletonMap("sysKey", "sysVal"))
        .setTwillRunId("twill-999")
        .setCluster(cluster)
        .setProfileId(ProfileId.NATIVE)
        .setPeerName("peer-1")
        .setSourceId(sourceId)
        .setArtifactId(artifactId)
        .setPrincipal("alice@example.com")
        .setFlowControlStatus("RUNNING")
        .build();

    RunRecordDetail copy = RunRecordDetail.builder(detail).build();
    Assert.assertEquals(detail, copy);
    Assert.assertEquals(detail.hashCode(), copy.hashCode());
    Assert.assertEquals(Collections.singletonMap("k", "v"), detail.getUserArgs());

    String expectedToString = "RunRecordDetail{"
        + "programRunId=" + runId
        + ", startTs=100"
        + ", runTs=110"
        + ", stopTs=210"
        + ", suspendTs=130"
        + ", resumeTs=140"
        + ", stoppingTs=190"
        + ", terminateTs=220"
        + ", status=COMPLETED"
        + ", twillrunid=twill-999"
        + ", systemArgs={sysKey=sysVal}"
        + ", properties={runtimeArgs={\"k\":\"v\"}}"
        + ", cluster=" + cluster
        + ", profile=" + ProfileId.NATIVE
        + ", peerName=peer-1"
        + ", sourceId=0aff"
        + ", artifactId=" + artifactId
        + ", principal=alice@example.com"
        + ", flowcontrolstatus=RUNNING"
        + "}";
    Assert.assertEquals(expectedToString, detail.toString());

    // Also verify PENDING record with null sourceId formats sourceId=null cleanly.
    RunRecordDetail pending = RunRecordDetail.builder()
        .setProgramRunId(runId)
        .setStartTime(50L)
        .setStatus(ProgramRunStatus.PENDING)
        .setProfileId(ProfileId.NATIVE)
        .build();
    Assert.assertTrue(pending.toString().contains("sourceId=null"));
  }
}
