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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link FlowControlService}.
 */
public class FlowControlServiceTest {

  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0");

  private static FlowControlService flowControlService;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    flowControlService = injector.getInstance(FlowControlService.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void tearDownClass() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void cleanupTest() {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore.create(context).deleteAllAppMetadataTables();
    });
  }

  @Test
  public void testAddRequestStoresSystemAndUserArgumentsSeparately() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("testApp").workflow("testWorkflow");
    ProgramRunId programRunId = programId.run(RunIds.generate());
    ProfileId sysProfile = NamespaceId.DEFAULT.profile("sysProfile");
    Map<String, String> systemArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, sysProfile.getScopedName(),
        ProgramOptionConstants.PRINCIPAL, "sysPrincipal",
        ProgramOptionConstants.PEER_NAME, "sysPeer",
        "system.only.key", "systemValue");
    Map<String, String> userArgs = ImmutableMap.of(
        "user.only.key", "userValue",
        ProgramOptionConstants.WORKFLOW_NAME, "userWorkflow",
        ProgramOptionConstants.WORKFLOW_RUN_ID, RunIds.generate().getId(),
        SystemArguments.PROFILE_NAME, "user:customProfile");
    ProgramOptions programOptions = new SimpleProgramOptions(
        programId, new BasicArguments(systemArgs), new BasicArguments(userArgs), false);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, null, ARTIFACT_ID);

    FlowControlService.Counter counter = flowControlService.addRequestAndGetCounter(
        programRunId, programOptions, programDescriptor);

    Assert.assertEquals(1, counter.getLaunchingCount());
    Assert.assertEquals(0, counter.getRunningCount());

    RunRecordDetail record = getRunRecord(programRunId);
    Assert.assertNotNull(record);
    Assert.assertEquals(ProgramRunStatus.PENDING, record.getStatus());
    Assert.assertEquals(systemArgs, record.getSystemArgs());
    Assert.assertEquals(userArgs, record.getUserArgs());
    Assert.assertEquals(sysProfile, record.getProfileId());
    Assert.assertEquals("sysPrincipal", record.getPrincipal());
    Assert.assertEquals("sysPeer", record.getPeerName());
    Assert.assertEquals("launching", record.getFlowControlStatus());
    Assert.assertNull(record.getProperties().get(AppMetadataStore.WORKFLOW_RUNID));
  }

  @Test
  public void testInnerWorkflowRunExcludedFromLaunchingCount() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("testApp").spark("innerSpark");
    ProgramRunId programRunId = programId.run(RunIds.generate());
    String workflowRunId = RunIds.generate().getId();
    Map<String, String> systemArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName(),
        ProgramOptionConstants.WORKFLOW_NAME, "parentWorkflow",
        ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);
    Map<String, String> userArgs = ImmutableMap.of("user.key", "userValue");
    ProgramOptions programOptions = new SimpleProgramOptions(
        programId, new BasicArguments(systemArgs), new BasicArguments(userArgs), false);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, null, ARTIFACT_ID);

    FlowControlService.Counter counter = flowControlService.addRequestAndGetCounter(
        programRunId, programOptions, programDescriptor);

    Assert.assertEquals(0, counter.getLaunchingCount());
    Assert.assertEquals(0, counter.getRunningCount());

    RunRecordDetail record = getRunRecord(programRunId);
    Assert.assertNotNull(record);
    Assert.assertEquals("", record.getFlowControlStatus());
    Assert.assertEquals(workflowRunId, record.getProperties().get(AppMetadataStore.WORKFLOW_RUNID));
    Assert.assertEquals(systemArgs, record.getSystemArgs());
    Assert.assertEquals(userArgs, record.getUserArgs());
  }

  @Test
  public void testStopBeforeProvisioningPreservesSystemAndUserArguments() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("testApp").workflow("stoppedWorkflow");
    ProgramRunId programRunId = programId.run(RunIds.generate());
    Map<String, String> systemArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName(),
        "system.key", "systemValue");
    Map<String, String> userArgs = ImmutableMap.of(
        "user.key", "userValue",
        "system.key", "userOverrideValue");
    ProgramOptions programOptions = new SimpleProgramOptions(
        programId, new BasicArguments(systemArgs), new BasicArguments(userArgs), false);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, null, ARTIFACT_ID);

    flowControlService.addRequestAndGetCounter(programRunId, programOptions, programDescriptor);

    long stopTs = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS) + 1L;
    byte[] sourceId = Bytes.toBytes(1L);
    RunRecordDetail stopped = TransactionRunners.run(transactionRunner, context -> {
      return AppMetadataStore.create(context).recordProgramStop(
          programRunId, stopTs, ProgramRunStatus.KILLED, null, sourceId);
    });
    Assert.assertNotNull(stopped);

    RunRecordDetail record = getRunRecord(programRunId);
    Assert.assertNotNull(record);
    Assert.assertEquals(ProgramRunStatus.KILLED, record.getStatus());
    Assert.assertEquals(systemArgs, record.getSystemArgs());
    Assert.assertEquals(userArgs, record.getUserArgs());
    Assert.assertEquals(0, flowControlService.getCounter().getLaunchingCount());
  }

  private RunRecordDetail getRunRecord(ProgramRunId programRunId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return AppMetadataStore.create(context).getRun(programRunId);
    });
  }
}
