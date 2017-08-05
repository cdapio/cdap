/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.WordCountApp;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Unit test for {@link RunRecordCorrectorService}
 */
public class RunRecordCorrectorServiceTest extends AppFabricTestBase {

  private static Store store;
  private static ProgramLifecycleService programLifecycleService;
  private static ProgramRuntimeService runtimeService;

  @BeforeClass
  public static void setup() throws Exception {
    store = getInjector().getInstance(DefaultStore.class);
    runtimeService = getInjector().getInstance(ProgramRuntimeService.class);
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
  }

  @Test
  public void testInvalidFlowRunRecord() throws Exception {
    // Create App with Flow and the deploy
    HttpResponse response = deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    final Id.Program wordcountFlow1 =
      Id.Program.from(TEST_NAMESPACE1, "WordCountApp", ProgramType.FLOW, "WordCountFlow");

    // flow is stopped initially
    Assert.assertEquals("STOPPED", getProgramStatus(wordcountFlow1));

    // start a flow and check the status
    startProgram(wordcountFlow1);
    waitState(wordcountFlow1, ProgramRunStatus.RUNNING.toString());

    // Wait until we have a run record
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(wordcountFlow1, ProgramRunStatus.RUNNING).size();
      }
    }, 5, TimeUnit.SECONDS);

    // Get the RunRecord
    List<RunRecord> runRecords = getProgramRuns(wordcountFlow1, ProgramRunStatus.RUNNING);
    Assert.assertEquals(1, runRecords.size());
    final RunRecord rr = runRecords.get(0);

    // Check the RunRecords status
    Assert.assertEquals(ProgramRunStatus.RUNNING, rr.getStatus());

    // Lets set the runtime info to off
    RuntimeInfo runtimeInfo = runtimeService.lookup(wordcountFlow1.toEntityId(), RunIds.fromString(rr.getPid()));
    ProgramController programController = runtimeInfo.getController();
    programController.stop();

    // Verify that the status of that run is KILLED
    Tasks.waitFor(ProgramRunStatus.KILLED, new Callable<ProgramRunStatus>() {
      @Override
      public ProgramRunStatus call() throws Exception {
        RunRecordMeta runRecord = store.getRun(wordcountFlow1.toEntityId(), rr.getPid());
        return runRecord == null ? null : runRecord.getStatus();
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Use the store manipulate state to be RUNNING
    long now = System.currentTimeMillis();
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);
    store.setStartAndRun(wordcountFlow1.toEntityId(), rr.getPid(), nowSecs, nowSecs + 1);

    // Now check again via Store to assume data store is wrong.
    RunRecord runRecordMeta = store.getRun(wordcountFlow1.toEntityId(), rr.getPid());
    Assert.assertNotNull(runRecordMeta);
    Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMeta.getStatus());

    // Verify there is NO FAILED run record for the application
    runRecords = getProgramRuns(wordcountFlow1, ProgramRunStatus.FAILED);
    Assert.assertEquals(0, runRecords.size());

    // Start the RunRecordCorrectorService, which will fix the run record
    new LocalRunRecordCorrectorService(store, programLifecycleService, runtimeService).startUp();

    // Verify there is one FAILED run record for the application
    runRecords = getProgramRuns(wordcountFlow1, ProgramRunStatus.FAILED);
    Assert.assertEquals(1, runRecords.size());
    Assert.assertEquals(ProgramRunStatus.FAILED, runRecords.get(0).getStatus());
  }
}
