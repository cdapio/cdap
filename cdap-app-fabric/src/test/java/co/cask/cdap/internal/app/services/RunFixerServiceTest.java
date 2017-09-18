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
import co.cask.cdap.WorkflowAppWithLocalDataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Unit test for {@link RunFixerService}
 */
public class RunFixerServiceTest extends AppFabricTestBase {

  private static Store store;
  private static ProgramStateWriter programStateWriter;
  private static ProgramLifecycleService programLifecycleService;
  private static ProgramRuntimeService runtimeService;
  private static NamespaceAdmin namespaceAdmin;
  private static DatasetFramework datasetFramework;

  @BeforeClass
  public static void setup() throws Exception {
    store = getInjector().getInstance(DefaultStore.class);
    programStateWriter = getInjector().getInstance(ProgramStateWriter.class);
    runtimeService = getInjector().getInstance(ProgramRuntimeService.class);
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    datasetFramework = getInjector().getInstance(DatasetFramework.class);
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
    // subtract one second so that the run corrector threshold will not filter it out
    long now = System.currentTimeMillis() - 1000L;
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);
    store.setStart(wordcountFlow1.toEntityId(), rr.getPid(), nowSecs, null, ImmutableMap.<String, String>of(),
                   ImmutableMap.<String, String>of(), ByteBuffer.allocate(0).array());
    store.setRunning(wordcountFlow1.toEntityId(), rr.getPid(), nowSecs + 1, null, ByteBuffer.allocate(1).array());

    // Now check again via Store to assume data store is wrong.
    RunRecord runRecordMeta = store.getRun(wordcountFlow1.toEntityId(), rr.getPid());
    Assert.assertNotNull(runRecordMeta);
    Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMeta.getStatus());

    // Verify there is NO FAILED run record for the application
    runRecords = getProgramRuns(wordcountFlow1, ProgramRunStatus.FAILED);
    Assert.assertEquals(0, runRecords.size());

    // Start the RunRecordCorrectorService, which will fix the run record
    CConfiguration testConf = CConfiguration.create();
    // set threshold to 0 so that it will actually correct the record
    testConf.set(Constants.AppFabric.PROGRAM_MAX_START_SECONDS, "0");
    new LocalRunFixerService(testConf, store, programStateWriter, programLifecycleService,
                                       runtimeService, namespaceAdmin, datasetFramework).startUp();

    // Wait for the FAILED run record for the application
    Tasks.waitFor(1, new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                      return getProgramRuns(wordcountFlow1, ProgramRunStatus.FAILED).size();
                    }
                  }, 30, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
  }

  @Test
  public void testLocalDatasetDeleteion() throws Exception {
    // Create App with Flow and the deploy
    HttpResponse response = deploy(WorkflowAppWithLocalDataset.class,
                                   Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    final ProgramId workflow = new NamespaceId(TEST_NAMESPACE1)
      .app(WorkflowAppWithLocalDataset.APP_NAME)
      .workflow(WorkflowAppWithLocalDataset.WORKFLOW_NAME);

    startProgram(workflow.toId(), ImmutableMap.of("dataset.*.keep.local", "true"));

    // Wait until we have a COMPLETED run record
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(workflow.toId(), ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.SECONDS);

    // Get the RunRecord
    List<RunRecord> runRecords = getProgramRuns(workflow.toId(), ProgramRunStatus.COMPLETED);

    Assert.assertEquals(1, runRecords.size());

    String pid = runRecords.get(0).getPid();

    // Get the local dataset specifications
    final Map<String, String> properties = ImmutableMap.of(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY, "true");
    Collection<DatasetSpecificationSummary> instances = datasetFramework.getInstances(new NamespaceId(TEST_NAMESPACE1),
                                                                                      properties);

    Assert.assertEquals(1, instances.size());
    DatasetSpecificationSummary summary = instances.iterator().next();
    Assert.assertTrue(summary.getName().endsWith(pid));

    // Update the dataset properties to remove keep.local so that local dataset deleter can delete it
    Map<String, String> updatedProperties = new HashMap<>();
    updatedProperties.putAll(summary.getProperties());
    updatedProperties.remove(Constants.AppFabric.WORKFLOW_KEEP_LOCAL);
    datasetFramework.updateInstance(new DatasetId(TEST_NAMESPACE1, summary.getName()),
                                    DatasetProperties.of(updatedProperties));

    // Start the local dataset deletion service now
    CConfiguration testConf = CConfiguration.create();
    // set threshold to 0 so that it will actually correct the record
    testConf.set(Constants.AppFabric.LOCAL_DATASET_DELETER_INTERVAL_SECONDS, "1");
    testConf.set(Constants.AppFabric.LOCAL_DATASET_DELETER_INITIAL_DELAY_SECONDS, "1");
    new LocalRunFixerService(testConf, store, programStateWriter, programLifecycleService,
                             runtimeService, namespaceAdmin, datasetFramework).startUp();

    // Wait for the deletion of the local dataset
    Tasks.waitFor(0, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return datasetFramework.getInstances(new NamespaceId(TEST_NAMESPACE1), properties).size();
      }
    }, 30, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
  }
}
