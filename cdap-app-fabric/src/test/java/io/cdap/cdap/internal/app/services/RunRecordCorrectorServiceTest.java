/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import io.cdap.cdap.WorkflowAppWithLocalDataset;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.app.runtime.AbstractProgramRuntimeService;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.NotRunningProgramLiveInfo;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Unit test for {@link RunRecordCorrectorService}
 */
public class RunRecordCorrectorServiceTest extends AppFabricTestBase {
  private static final Map<String, String> SINGLETON_PROFILE_MAP =
    Collections.singletonMap(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());

  private static Store store;
  private static CConfiguration cConf;
  private static ProgramStateWriter programStateWriter;
  private static ProgramRuntimeService runtimeService;
  private static NamespaceAdmin namespaceAdmin;
  private static DatasetFramework datasetFramework;

  @BeforeClass
  public static void setup() {
    store = getInjector().getInstance(DefaultStore.class);
    cConf = getInjector().getInstance(CConfiguration.class);
    programStateWriter = getInjector().getInstance(ProgramStateWriter.class);
    runtimeService = getInjector().getInstance(ProgramRuntimeService.class);
    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    datasetFramework = getInjector().getInstance(DatasetFramework.class);
  }

  @Test
  public void testFixProgram() throws Exception {
    final AtomicInteger sourceId = new AtomicInteger(0);

    // Write 10 services with starting state
    // Write 10 workers with running state
    Map<ProgramRunId, ProgramRunStatus> expectedStates = new HashMap<>();
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    for (int i = 0; i < 10; i++) {
      ProgramRunId serviceId = NamespaceId.DEFAULT.app("test").service("service" + i).run(randomRunId());
      store.setProvisioning(serviceId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                            Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
      store.setProvisioned(serviceId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
      store.setStart(serviceId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
      expectedStates.put(serviceId, ProgramRunStatus.FAILED);

      ProgramRunId workerId = new NamespaceId("ns").app("test").worker("worker" + i).run(randomRunId());
      store.setProvisioning(workerId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                            Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
      store.setProvisioned(workerId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
      store.setStart(workerId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
      store.setRunning(workerId, System.currentTimeMillis(), null, Bytes.toBytes(sourceId.getAndIncrement()));
      expectedStates.put(workerId, ProgramRunStatus.FAILED);
    }

    // Write a flow with suspend state
    ProgramRunId flowId = new NamespaceId("ns").app("test").service("flow").run(randomRunId());
    store.setProvisioning(flowId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(flowId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(flowId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(flowId, System.currentTimeMillis(),
                     null, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setSuspend(flowId, Bytes.toBytes(sourceId.getAndIncrement()), -1);
    expectedStates.put(flowId, ProgramRunStatus.SUSPENDED);

    // Write two MR in starting state. One with workflow information, one without.
    ProgramRunId mrId = NamespaceId.DEFAULT.app("app").mr("mr").run(randomRunId());
    store.setProvisioning(mrId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(mrId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(mrId, null, Collections.emptyMap(), Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(mrId, ProgramRunStatus.FAILED);

    ProgramRunId workflowId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(randomRunId());
    ProgramRunId mrInWorkflowId =
      workflowId.getParent().getParent().mr("mrInWorkflow").run(randomRunId());
    store.setProvisioning(mrInWorkflowId, Collections.emptyMap(),
                          ImmutableMap.of(
                            ProgramOptionConstants.WORKFLOW_NAME, workflowId.getProgram(),
                            ProgramOptionConstants.WORKFLOW_RUN_ID, workflowId.getRun(),
                            ProgramOptionConstants.WORKFLOW_NODE_ID, "mr",
                            SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName()
                          ),
                          Bytes.toBytes(sourceId.getAndIncrement()),
                          artifactId);
    store.setProvisioned(mrInWorkflowId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(mrInWorkflowId, null,
                   ImmutableMap.of(
                     ProgramOptionConstants.WORKFLOW_NAME, workflowId.getProgram(),
                     ProgramOptionConstants.WORKFLOW_RUN_ID, workflowId.getRun(),
                     ProgramOptionConstants.WORKFLOW_NODE_ID, "mr"
                   ),
                   Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(workflowId, ProgramRunStatus.STARTING);

    // Write the workflow in RUNNING state.
    store.setProvisioning(workflowId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                          Bytes.toBytes(sourceId.getAndIncrement()), artifactId);
    store.setProvisioned(workflowId, 0, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setStart(workflowId, null, Collections.emptyMap(),
                   Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(workflowId, System.currentTimeMillis(), null, Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(workflowId, ProgramRunStatus.RUNNING);

    // Use a ProgramRuntimeService that only reports running state based on a set of know ids
    final Map<ProgramId, RunId> runningSet = new HashMap<>();
    ProgramRuntimeService programRuntimeService = new AbstractProgramRuntimeService(cConf, null, null,
                                                                                    new NoOpProgramStateWriter()) {

      @Override
      public ProgramLiveInfo getLiveInfo(ProgramId programId) {
        return new NotRunningProgramLiveInfo(programId);
      }

      @Override
      public Map<RunId, RuntimeInfo> list(ProgramId program) {
        RunId runId = runningSet.get(program);
        if (runId != null) {
          RuntimeInfo runtimeInfo = new SimpleRuntimeInfo(null, program);
          return Collections.singletonMap(runId, runtimeInfo);
        }
        return Collections.emptyMap();
      }
    };

    // Have both flow and workflow running
    runningSet.put(flowId.getParent(), RunIds.fromString(flowId.getRun()));
    runningSet.put(workflowId.getParent(), RunIds.fromString(workflowId.getRun()));

    ProgramStateWriter programStateWriter = new NoOpProgramStateWriter() {
      @Override
      public void error(ProgramRunId programRunId, Throwable failureCause) {
        store.setStop(programRunId, System.currentTimeMillis(),
                      ProgramRunStatus.FAILED, new BasicThrowable(failureCause),
                      Bytes.toBytes(sourceId.getAndIncrement()));
      }
    };

    // Create a run record fixer.
    // Set the start buffer time to -1 so that it fixes right away.
    // Also use a small tx batch size to validate the batching logic.
    RunRecordCorrectorService fixer = new RunRecordCorrectorService(cConf, store, programStateWriter,
                                                                    programRuntimeService,
                                                                    namespaceAdmin, datasetFramework,
                                                                    -1L, 5) { };
    fixer.fixRunRecords();

    // Validates all expected states
    for (Map.Entry<ProgramRunId, ProgramRunStatus> entry : expectedStates.entrySet()) {
      validateExpectedState(entry.getKey(), entry.getValue());
    }

    // Remove the workflow from the running set and mark it as completed
    runningSet.remove(workflowId.getParent());
    store.setStop(workflowId, System.currentTimeMillis(),
                  ProgramRunStatus.COMPLETED, Bytes.toBytes(sourceId.getAndIncrement()));

    fixer.fixRunRecords();

    // Both the workflow and the MR in workflow should be changed to failed state
    expectedStates.put(workflowId, ProgramRunStatus.COMPLETED);
    expectedStates.put(mrInWorkflowId, ProgramRunStatus.FAILED);

    // Validates all expected states again
    for (Map.Entry<ProgramRunId, ProgramRunStatus> entry : expectedStates.entrySet()) {
      validateExpectedState(entry.getKey(), entry.getValue());
    }
  }

  private void validateExpectedState(ProgramRunId programRunId, ProgramRunStatus expectedStatus) throws Exception {
    Tasks.waitFor(ImmutablePair.of(programRunId, expectedStatus), () -> {
      RunRecordDetail runRecord = store.getRun(programRunId);
      return ImmutablePair.of(programRunId, runRecord == null ? null : runRecord.getStatus());
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testLocalDatasetDeleteion() throws Exception {
    // Deploy an app
    deploy(WorkflowAppWithLocalDataset.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);

    final ProgramId workflow = new NamespaceId(TEST_NAMESPACE1)
      .app(WorkflowAppWithLocalDataset.APP_NAME)
      .workflow(WorkflowAppWithLocalDataset.WORKFLOW_NAME);

    startProgram(Id.Program.fromEntityId(workflow), ImmutableMap.of("dataset.*.keep.local", "true"));

    // Wait until we have a COMPLETED run record
    Tasks.waitFor(1, () -> getProgramRuns(Id.Program.fromEntityId(workflow), ProgramRunStatus.COMPLETED).size(),
                  5, TimeUnit.SECONDS);

    // Get the RunRecord
    List<RunRecord> runRecords = getProgramRuns(Id.Program.fromEntityId(workflow), ProgramRunStatus.COMPLETED);

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
    Map<String, String> updatedProperties = new HashMap<>(summary.getProperties());
    updatedProperties.remove(Constants.AppFabric.WORKFLOW_KEEP_LOCAL);
    datasetFramework.updateInstance(new DatasetId(TEST_NAMESPACE1, summary.getName()),
                                    DatasetProperties.of(updatedProperties));

    // Start the local dataset deletion service now
    CConfiguration testConf = CConfiguration.create();
    // set threshold to 0 so that it will actually correct the record
    testConf.set(Constants.AppFabric.LOCAL_DATASET_DELETER_INTERVAL_SECONDS, "1");
    testConf.set(Constants.AppFabric.LOCAL_DATASET_DELETER_INITIAL_DELAY_SECONDS, "1");
    new LocalRunRecordCorrectorService(testConf, store, programStateWriter,
                                       runtimeService, namespaceAdmin, datasetFramework).startUp();
//
    // Wait for the deletion of the local dataset
    Tasks.waitFor(0, () -> datasetFramework.getInstances(new NamespaceId(TEST_NAMESPACE1), properties).size(),
                  30, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
  }

  private RunId randomRunId() {
    long startTime = ThreadLocalRandom.current().nextLong(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    return RunIds.generate(startTime);
  }
}
