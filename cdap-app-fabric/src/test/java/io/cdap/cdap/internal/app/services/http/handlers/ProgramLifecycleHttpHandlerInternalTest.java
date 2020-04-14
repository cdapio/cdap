/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import io.cdap.cdap.SleepingWorkflowApp;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.ProgramLifecycleHttpHandlerInternal;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Test for {@link ProgramLifecycleHttpHandlerInternal}
 */
public class ProgramLifecycleHttpHandlerInternalTest extends AppFabricTestBase {
  private static final String STOPPED = "STOPPED";
  private static final String RUNNING = "RUNNING";

  private static ProgramRunRecordFetcher programRunRecordFetcher = null;

  @BeforeClass
  public static void init() {
    programRunRecordFetcher = new RemoteProgramRunRecordFetcher(
      getInjector().getInstance(DiscoveryServiceClient.class));
  }

  @Test
  public void testGetRunRecordMeta() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String application = SleepingWorkflowApp.NAME;
    String program = "SleepWorkflow";
    ProgramId programId = new ProgramId(namespace, application, ProgramType.WORKFLOW, program);
    ProgramRunId programRunId = null;
    String runId = null;

    // Deploy an app containing a workflow
    deploy(SleepingWorkflowApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Run program once
    startProgram(programId);
    waitState(programId, RUNNING);
    waitState(programId, STOPPED);

    // Get RunRecord via public REST API
    List<RunRecord> runRecordList = getProgramRuns(programId, ProgramRunStatus.COMPLETED);
    Assert.assertEquals(1, runRecordList.size());
    runId = runRecordList.get(0).getPid();

    // Get the RunRecordDetail via internal REST API and verify
    programRunId = new ProgramRunId(namespace, application, ProgramType.WORKFLOW, program, runId);
    RunRecordDetail runRecordMeta = programRunRecordFetcher.getRunRecordMeta(programRunId);
    Assert.assertTrue(runRecordMeta.getProperties().size() > 0);
    Assert.assertNotNull(runRecordMeta.getCluster());
    Assert.assertEquals(ProfileId.NATIVE, runRecordMeta.getProfileId());

    // cleanup
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
                                                         Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getResponseCode());
  }
}
