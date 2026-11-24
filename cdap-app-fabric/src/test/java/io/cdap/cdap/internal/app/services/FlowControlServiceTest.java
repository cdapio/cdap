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
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link FlowControlService}.
 */
public class FlowControlServiceTest {

  private static Injector injector;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setupClass() {
    injector = AppFabricTestHelper.getInjector();
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void cleanupTest() {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore.create(context).deleteAllAppMetadataTables();
    });
  }

  @Test
  public void testAddRequestRecordsPendingRunArguments() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app").workflow("workflow")
        .run(RunIds.generate());
    Map<String, String> systemArgs = ImmutableMap.of(SystemArguments.PROFILE_NAME,
        ProfileId.NATIVE.getScopedName());
    Map<String, String> userArgs = ImmutableMap.of("key", "value");
    SimpleProgramOptions programOptions = new SimpleProgramOptions(runId.getParent(),
        new BasicArguments(systemArgs), new BasicArguments(userArgs));
    ProgramDescriptor programDescriptor = new ProgramDescriptor(runId.getParent(), null,
        NamespaceId.DEFAULT.artifact("artifact", "1.0.0"));

    injector.getInstance(FlowControlService.class)
        .addRequestAndGetCounter(runId, programOptions, programDescriptor);

    RunRecordDetail runRecord = TransactionRunners.run(transactionRunner, context -> {
      return AppMetadataStore.create(context).getRun(runId);
    });
    Assert.assertNotNull(runRecord);
    Assert.assertEquals(ProgramRunStatus.PENDING, runRecord.getStatus());
    Assert.assertEquals(systemArgs, runRecord.getSystemArgs());
    Assert.assertEquals(ProfileId.NATIVE, runRecord.getProfileId());
    Assert.assertEquals(userArgs, runRecord.getUserArgs());
  }

  @Test
  public void testWorkflowChildRunIsNotCountedAsLaunching() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());
    SimpleProgramOptions programOptions = new SimpleProgramOptions(runId.getParent(),
        new BasicArguments(ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NAME, "workflow")),
        new BasicArguments());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(runId.getParent(), null,
        NamespaceId.DEFAULT.artifact("artifact", "1.0.0"));

    FlowControlService.Counter counter = injector.getInstance(FlowControlService.class)
        .addRequestAndGetCounter(runId, programOptions, programDescriptor);

    Assert.assertEquals(0, counter.getLaunchingCount());
  }
}
