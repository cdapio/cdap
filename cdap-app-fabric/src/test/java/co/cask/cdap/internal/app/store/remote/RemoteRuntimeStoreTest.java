/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests implementation of {@link RemoteRuntimeStore}, by using it to perform writes/updates, and then using a
 * local {@link DefaultStore} to verify the updates/writes.
 */
public class RemoteRuntimeStoreTest extends AppFabricTestBase {

  private static DefaultStore store;
  private static RemoteRuntimeStore runtimeStore;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    store = injector.getInstance(DefaultStore.class);
    runtimeStore = injector.getInstance(RemoteRuntimeStore.class);
  }

  @Test
  public void testSimpleCase() {
    ProgramId flowId = new ProgramId(Id.Namespace.DEFAULT.getId(), "test_app", ProgramType.FLOW, "test_flow");
    long stopTime = System.currentTimeMillis() / 2000;
    long startTime = stopTime - 20;
    String pid = RunIds.generate(startTime * 1000).getId();
    // to test null serialization (setStart can take in nullable)
    String twillRunId = null;
    Map<String, String> runtimeArgs = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of("runtimeArgs", GSON.toJson(runtimeArgs));
    Map<String, String> systemArgs = ImmutableMap.of("a", "b");
    RunRecordMeta initialRunRecord =
      new RunRecordMeta(pid, startTime, null, ProgramRunStatus.RUNNING, properties, systemArgs, twillRunId);

    runtimeStore.setStart(flowId, pid, startTime, twillRunId, runtimeArgs, systemArgs);
    RunRecordMeta runMeta = store.getRun(flowId, pid);
    Assert.assertEquals(initialRunRecord, runMeta);

    runtimeStore.setSuspend(flowId, pid);
    Assert.assertEquals(new RunRecordMeta(initialRunRecord, null, ProgramRunStatus.SUSPENDED),
                        store.getRun(flowId, pid));

    runtimeStore.setResume(flowId, pid);
    Assert.assertEquals(initialRunRecord,
                        store.getRun(flowId, pid));

    // this should be a no-op, since the status is actually RUNNING, and not SUSPENDED
    runtimeStore.compareAndSetStatus(flowId, pid, ProgramRunStatus.SUSPENDED, ProgramRunStatus.COMPLETED);
    Assert.assertEquals(initialRunRecord,
                        store.getRun(flowId, pid));

    // this call to compareAndSetStatus will correctly mark it as COMPLETED.
    // we don't do direct equals comparison on the run record objects, because the method internally sets the stopTime
    runtimeStore.compareAndSetStatus(flowId, pid, ProgramRunStatus.RUNNING, ProgramRunStatus.COMPLETED);
    RunRecordMeta runRecordMeta = store.getRun(flowId, pid);
    Assert.assertEquals(initialRunRecord.getStartTs(), runRecordMeta.getStartTs());
    Assert.assertEquals(initialRunRecord.getPid(), runRecordMeta.getPid());
    Assert.assertEquals(initialRunRecord.getTwillRunId(), runRecordMeta.getTwillRunId());
    Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecordMeta.getStatus());
  }

  @Test
  public void testWorkflowMethods() {
    ProgramId workflowId =
      new ProgramId(Id.Namespace.DEFAULT.getId(), "test_app", ProgramType.WORKFLOW, "test_workflow");
    long stopTime = System.currentTimeMillis() / 1000;
    long startTime = stopTime - 20;
    String pid = RunIds.generate(startTime * 1000).getId();
    String twillRunId = "twill_run_id";
    Map<String, String> runtimeArgs = ImmutableMap.of();
    Map<String, String> properties = ImmutableMap.of("runtimeArgs", GSON.toJson(runtimeArgs));
    Map<String, String> systemArgs = ImmutableMap.of();
    RunRecordMeta initialRunRecord =
      new RunRecordMeta(pid, startTime, null, ProgramRunStatus.RUNNING, properties, systemArgs, twillRunId);

    runtimeStore.setStart(workflowId, pid, startTime, twillRunId, runtimeArgs, systemArgs);
    Assert.assertEquals(initialRunRecord, store.getRun(workflowId, pid));

    ProgramId mapreduceId =
      new ProgramId(workflowId.getNamespace(), workflowId.getApplication(), ProgramType.MAPREDUCE, "test_mr");
    String mapreducePid = RunIds.generate(startTime * 1000).getId();
    // these system properties just have to be set on the system arguments of the program, in order for it to be
    // understood as a program in a workflow node
    Map<String, String> mrSystemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, "test_node_id",
                                                       ProgramOptionConstants.WORKFLOW_NAME, workflowId.getProgram(),
                                                       ProgramOptionConstants.WORKFLOW_RUN_ID, pid);
    runtimeStore.setStart(mapreduceId, mapreducePid, startTime, twillRunId, runtimeArgs, mrSystemArgs);

    BasicThrowable failureCause =
      new BasicThrowable(new IllegalArgumentException("failure", new RuntimeException("oops")));
    runtimeStore.setStop(mapreduceId, mapreducePid, stopTime, ProgramRunStatus.FAILED, failureCause);

    runtimeStore.setStop(workflowId, pid, stopTime, ProgramRunStatus.FAILED);

    RunRecordMeta completedWorkflowRecord = store.getRun(workflowId, pid);
    // we're not comparing properties, since runtime (such as starting/stopping inner programs) modifies it
    Assert.assertEquals(pid, completedWorkflowRecord.getPid());
    Assert.assertEquals(initialRunRecord.getStartTs(), completedWorkflowRecord.getStartTs());
    Assert.assertEquals((Long) stopTime, completedWorkflowRecord.getStopTs());
    Assert.assertEquals(ProgramRunStatus.FAILED, completedWorkflowRecord.getStatus());
    Assert.assertEquals(twillRunId, completedWorkflowRecord.getTwillRunId());
    Assert.assertEquals(systemArgs, completedWorkflowRecord.getSystemArgs());

    // test that the BasicThrowable was serialized properly by RemoteRuntimeStore
    ProgramRunId workflowRunId = workflowId.run(pid);
    List<WorkflowNodeStateDetail> workflowNodeStates = store.getWorkflowNodeStates(workflowRunId);
    Assert.assertEquals(1, workflowNodeStates.size());
    WorkflowNodeStateDetail workflowNodeStateDetail = workflowNodeStates.get(0);
    Assert.assertEquals("test_node_id", workflowNodeStateDetail.getNodeId());
    Assert.assertEquals(mapreducePid, workflowNodeStateDetail.getRunId());
    Assert.assertEquals(NodeStatus.FAILED, workflowNodeStateDetail.getNodeStatus());
    Assert.assertEquals(failureCause, workflowNodeStateDetail.getFailureCause());
  }
}
