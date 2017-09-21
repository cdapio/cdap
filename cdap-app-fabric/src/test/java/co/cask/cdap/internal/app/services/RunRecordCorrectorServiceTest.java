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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.app.runtime.AbstractProgramRuntimeService;
import co.cask.cdap.app.runtime.NoOpProgramStateWriter;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Unit test for {@link RunRecordCorrectorService}
 */
public class RunRecordCorrectorServiceTest extends AppFabricTestBase {

  private static Store store;
  private static CConfiguration cConf;

  @BeforeClass
  public static void setup() throws Exception {
    store = getInjector().getInstance(DefaultStore.class);
    cConf = getInjector().getInstance(CConfiguration.class);
  }

  @Test
  public void testFixProgram() {
    final AtomicInteger sourceId = new AtomicInteger(0);

    // Write 10 services with starting state
    // Write 10 workers with running state
    Map<ProgramRunId, ProgramRunStatus> expectedStates = new HashMap<>();

    for (int i = 0; i < 10; i++) {
      ProgramRunId serviceId = NamespaceId.DEFAULT.app("test").service("service" + i).run(RunIds.generate());
      store.setStart(serviceId.getParent(), serviceId.getRun(), RunIds.getTime(serviceId.getRun(), TimeUnit.SECONDS),
                     null, Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(),
                     Bytes.toBytes(sourceId.getAndIncrement()));
      expectedStates.put(serviceId, ProgramRunStatus.FAILED);

      ProgramRunId workerId = new NamespaceId("ns").app("test").service("worker" + i).run(RunIds.generate());
      store.setStart(workerId.getParent(), workerId.getRun(), RunIds.getTime(workerId.getRun(), TimeUnit.SECONDS),
                     null, Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(),
                     Bytes.toBytes(sourceId.getAndIncrement()));
      store.setRunning(workerId.getParent(), workerId.getRun(), System.currentTimeMillis(),
                       null, Bytes.toBytes(sourceId.getAndIncrement()));
      expectedStates.put(workerId, ProgramRunStatus.FAILED);
    }

    // Write a flow with suspend state
    ProgramRunId flowId = new NamespaceId("ns").app("test").service("flow").run(RunIds.generate());
    store.setStart(flowId.getParent(), flowId.getRun(), RunIds.getTime(flowId.getRun(), TimeUnit.SECONDS),
                   null, Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(),
                   Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(flowId.getParent(), flowId.getRun(), System.currentTimeMillis(),
                     null, Bytes.toBytes(sourceId.getAndIncrement()));
    store.setSuspend(flowId.getParent(), flowId.getRun(), Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(flowId, ProgramRunStatus.SUSPENDED);

    // Write two MR in starting state. One with workflow information, one without.
    ProgramRunId mrId = NamespaceId.DEFAULT.app("app").mr("mr").run(RunIds.generate());
    store.setStart(mrId.getParent(), mrId.getRun(), RunIds.getTime(mrId.getRun(), TimeUnit.SECONDS),
                   null, Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(),
                   Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(mrId, ProgramRunStatus.FAILED);

    ProgramRunId workflowId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    ProgramRunId mrInWorkflowId = workflowId.getParent().getParent().mr("mrInWorkflow").run(RunIds.generate());
    store.setStart(mrInWorkflowId.getParent(), mrInWorkflowId.getRun(),
                   RunIds.getTime(mrInWorkflowId.getRun(), TimeUnit.SECONDS),
                   null, Collections.<String, String>emptyMap(),
                   ImmutableMap.of(
                     ProgramOptionConstants.WORKFLOW_NAME, workflowId.getProgram(),
                     ProgramOptionConstants.WORKFLOW_RUN_ID, workflowId.getRun(),
                     ProgramOptionConstants.WORKFLOW_NODE_ID, "mr"
                   ),
                   Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(workflowId, ProgramRunStatus.STARTING);

    // Write the workflow in RUNNING state.
    store.setStart(workflowId.getParent(), workflowId.getRun(), RunIds.getTime(workflowId.getRun(), TimeUnit.SECONDS),
                   null, Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap(),
                   Bytes.toBytes(sourceId.getAndIncrement()));
    store.setRunning(workflowId.getParent(), workflowId.getRun(), System.currentTimeMillis(),
                     null, Bytes.toBytes(sourceId.getAndIncrement()));
    expectedStates.put(workflowId, ProgramRunStatus.RUNNING);

    // Use a ProgramRuntimeService that only reports running state based on a set of know ids
    final Map<ProgramId, RunId> runningSet = new HashMap<>();
    ProgramRuntimeService programRuntimeService = new AbstractProgramRuntimeService(cConf, null, null, null) {

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
        store.setStop(programRunId.getParent(), programRunId.getRun(), System.currentTimeMillis(),
                      ProgramRunStatus.FAILED, new BasicThrowable(failureCause),
                      Bytes.toBytes(sourceId.getAndIncrement()));
      }
    };

    // Create a run record fixer.
    // Set the start buffer time to -1 so that it fixes right away.
    // Also use a small tx batch size to validate the batching logic.
    RunRecordCorrectorService fixer = new RunRecordCorrectorService(store, programStateWriter,
                                                                    programRuntimeService, -1L, 5) { };
    fixer.fixRunRecords();

    // Validates all expected states
    for (Map.Entry<ProgramRunId, ProgramRunStatus> entry : expectedStates.entrySet()) {
      validateExpectedState(entry.getKey(), entry.getValue());
    }

    // Remove the workflow from the running set and mark it as completed
    runningSet.remove(workflowId.getParent());
    store.setStop(workflowId.getParent(), workflowId.getRun(), System.currentTimeMillis(),
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

  private void validateExpectedState(ProgramRunId programRunId, ProgramRunStatus expectedStatus) {
    RunRecordMeta runRecord = store.getRun(programRunId.getParent(), programRunId.getRun());
    Assert.assertNotNull(runRecord);
    Assert.assertEquals(expectedStatus, runRecord.getStatus());
  }
}
