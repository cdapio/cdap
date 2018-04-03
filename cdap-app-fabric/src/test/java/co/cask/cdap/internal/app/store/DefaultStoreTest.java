/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.AppWithNoServices;
import co.cask.cdap.AppWithServices;
import co.cask.cdap.AppWithWorker;
import co.cask.cdap.FlowMapReduceApp;
import co.cask.cdap.NoProgramsApp;
import co.cask.cdap.ToyApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunCluster;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.store.DefaultNamespaceStore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.inject.Injector;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link DefaultStore}.
 */
public class DefaultStoreTest {
  private static final Gson GSON = new Gson();
  private static DefaultStore store;
  private static DefaultNamespaceStore nsStore;

  private int sourceId;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(DefaultStore.class);
    nsStore = injector.getInstance(DefaultNamespaceStore.class);
  }

  @Before
  public void before() throws Exception {
    store.clear();
    NamespacedLocationFactory namespacedLocationFactory =
      AppFabricTestHelper.getInjector().getInstance(NamespacedLocationFactory.class);
    namespacedLocationFactory.get(NamespaceId.DEFAULT).delete(true);
    NamespaceAdmin admin = AppFabricTestHelper.getInjector().getInstance(NamespaceAdmin.class);
    admin.create(NamespaceMeta.DEFAULT);
  }

  private void setStartAndRunning(final ProgramRunId id, final long startTime) {
    setStartAndRunning(id, startTime, ImmutableMap.of(), ImmutableMap.of());

  }

  private void setStartAndRunning(final ProgramRunId id, final long startTime,
                                  final Map<String, String> runtimeArgs,
                                  final Map<String, String> systemArgs) {
    setStart(id, startTime, runtimeArgs, systemArgs);
    store.setRunning(id, startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }

  private void setStart(final ProgramRunId id, final long startTime,
                        final Map<String, String> runtimeArgs,
                        final Map<String, String> systemArgs) {
    store.setProvisioning(id, startTime, runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
    store.setProvisioned(id, 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id, startTime, null, runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
  }

  @Test
  public void testLoadingProgram() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new ToyApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    store.addApplication(appId, appSpec);

    ProgramDescriptor descriptor = store.loadProgram(appId.flow("ToyFlow"));
    Assert.assertNotNull(descriptor);
    FlowSpecification flowSpec = descriptor.getSpecification();
    Assert.assertEquals("ToyFlow", flowSpec.getName());
  }

  @Test
  public void testStopBeforeStart() throws RuntimeException {
    ProgramId programId = new ProgramId("account1", "invalidApp", ProgramType.FLOW, "InvalidFlowOperation");
    long now = System.currentTimeMillis();
    String pid = RunIds.generate().getId();
    store.setStop(programId.run(pid), now, ProgramController.State.ERROR.getRunStatus(),
                  ByteBuffer.allocate(0).array());
    Assert.assertNull(store.getRun(programId.run(pid)));
  }

  @Test
  public void testDeleteSuspendedWorkflow() {
    NamespaceId namespaceId = new NamespaceId("namespace1");

    // Test delete application
    ApplicationId appId1 = namespaceId.app("app1");
    ProgramId programId1 = appId1.workflow("pgm1");
    RunId run1 = RunIds.generate();
    setStartAndRunning(programId1.run(run1.getId()), runIdToSecs(run1));
    store.setSuspend(programId1.run(run1.getId()), AppFabricTestHelper.createSourceId(++sourceId));
    store.removeApplication(appId1);
    Assert.assertTrue(store.getRuns(programId1, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).isEmpty());

    // Test delete namespace
    ProgramId programId2 = namespaceId.app("app2").workflow("pgm2");
    RunId run2 = RunIds.generate();
    setStartAndRunning(programId2.run(run2.getId()), runIdToSecs(run2));
    store.setSuspend(programId2.run(run2.getId()), AppFabricTestHelper.createSourceId(++sourceId));
    store.removeAll(namespaceId);
    nsStore.delete(namespaceId);
    Assert.assertTrue(store.getRuns(programId2, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).isEmpty());
  }

  @Test
  public void testWorkflowNodeState() throws Exception {
    String namespaceName = "namespace1";
    String appName = "app1";
    String workflowName = "workflow1";
    String mapReduceName = "mapReduce1";
    String sparkName = "spark1";

    ApplicationId appId = Ids.namespace(namespaceName).app(appName);
    ProgramId mapReduceProgram = appId.mr(mapReduceName);
    ProgramId sparkProgram = appId.spark(sparkName);

    long currentTime = System.currentTimeMillis();
    String workflowRunId = RunIds.generate(currentTime).getId();
    ProgramRunId workflowRun = appId.workflow(workflowName).run(workflowRunId);

    // start Workflow
    setStartAndRunning(workflowRun, currentTime);

    // start MapReduce as a part of Workflow
    Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, mapReduceName,
                                                     ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                                     ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);

    RunId mapReduceRunId = RunIds.generate(currentTime + 10);

    setStartAndRunning(mapReduceProgram.run(mapReduceRunId.getId()), currentTime + 10, ImmutableMap.of(), systemArgs);

    // stop the MapReduce program
    store.setStop(mapReduceProgram.run(mapReduceRunId.getId()), currentTime + 50, ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    // start Spark program as a part of Workflow
    systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, sparkName,
                                 ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                 ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);

    RunId sparkRunId = RunIds.generate(currentTime + 60);
    setStartAndRunning(sparkProgram.run(sparkRunId.getId()), currentTime + 60,
                       ImmutableMap.of(), systemArgs);

    // stop the Spark program with failure
    NullPointerException npe = new NullPointerException("dataset not found");
    IllegalArgumentException iae = new IllegalArgumentException("illegal argument", npe);
    store.setStop(sparkProgram.run(sparkRunId.getId()), currentTime + 100, ProgramRunStatus.FAILED,
                  new BasicThrowable(iae), AppFabricTestHelper.createSourceId(++sourceId));

    // stop Workflow
    store.setStop(workflowRun, currentTime + 110, ProgramRunStatus.FAILED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    List<WorkflowNodeStateDetail> nodeStateDetails = store.getWorkflowNodeStates(workflowRun);
    Map<String, WorkflowNodeStateDetail> workflowNodeStates = new HashMap<>();
    for (WorkflowNodeStateDetail nodeStateDetail : nodeStateDetails) {
      workflowNodeStates.put(nodeStateDetail.getNodeId(), nodeStateDetail);
    }

    Assert.assertEquals(2, workflowNodeStates.size());
    WorkflowNodeStateDetail nodeStateDetail = workflowNodeStates.get(mapReduceName);
    Assert.assertEquals(mapReduceName, nodeStateDetail.getNodeId());
    Assert.assertEquals(NodeStatus.COMPLETED, nodeStateDetail.getNodeStatus());
    Assert.assertEquals(mapReduceRunId.getId(), nodeStateDetail.getRunId());
    Assert.assertNull(nodeStateDetail.getFailureCause());

    nodeStateDetail = workflowNodeStates.get(sparkName);
    Assert.assertEquals(sparkName, nodeStateDetail.getNodeId());
    Assert.assertEquals(NodeStatus.FAILED, nodeStateDetail.getNodeStatus());
    Assert.assertEquals(sparkRunId.getId(), nodeStateDetail.getRunId());
    BasicThrowable failureCause = nodeStateDetail.getFailureCause();
    Assert.assertNotNull(failureCause);
    Assert.assertTrue("illegal argument".equals(failureCause.getMessage()));
    Assert.assertTrue("java.lang.IllegalArgumentException".equals(failureCause.getClassName()));
    failureCause = failureCause.getCause();
    Assert.assertNotNull(failureCause);
    Assert.assertTrue("dataset not found".equals(failureCause.getMessage()));
    Assert.assertTrue("java.lang.NullPointerException".equals(failureCause.getClassName()));
    Assert.assertNull(failureCause.getCause());
  }

  @Test
  public void testConcurrentStopStart() throws Exception {
    // Two programs that start/stop at same time
    // Should have two run history.
    ProgramId programId = new ProgramId("account1", "concurrentApp", ProgramType.FLOW, "concurrentFlow");
    long now = System.currentTimeMillis();
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);

    RunId run1 = RunIds.generate(now - 10000);
    setStartAndRunning(programId.run(run1.getId()), runIdToSecs(run1));
    RunId run2 = RunIds.generate(now - 10000);
    setStartAndRunning(programId.run(run2.getId()), runIdToSecs(run2));

    store.setStop(programId.run(run1.getId()), nowSecs, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));
    store.setStop(programId.run(run2.getId()), nowSecs, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    Map<ProgramRunId, RunRecordMeta> historymap = store.getRuns(programId, ProgramRunStatus.ALL,
                                                                0, Long.MAX_VALUE, Integer.MAX_VALUE);

    Assert.assertEquals(2, historymap.size());
  }

  @Test
  public void testLogProgramRunHistory() throws Exception {
    Map<String, String> noRuntimeArgsProps = ImmutableMap.of("runtimeArgs",
                                                             GSON.toJson(ImmutableMap.<String, String>of()));
    // record finished flow
    ProgramId programId = new ProgramId("account1", "application1", ProgramType.FLOW, "flow1");
    long now = System.currentTimeMillis();
    long startTimeSecs = TimeUnit.MILLISECONDS.toSeconds(now);
    // Time after program has been marked started to be marked running
    long startTimeDelaySecs = 1;

    RunId run1 = RunIds.generate(now - 20000);
    setStartAndRunning(programId.run(run1.getId()), runIdToSecs(run1));
    store.setStop(programId.run(run1.getId()), startTimeSecs - 10, ProgramController.State.ERROR.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    // record another finished flow
    RunId run2 = RunIds.generate(now - 10000);
    setStartAndRunning(programId.run(run2.getId()), runIdToSecs(run2));
    store.setStop(programId.run(run2.getId()), startTimeSecs - 5, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    // record a suspended flow
    RunId run21 = RunIds.generate(now - 7500);
    setStartAndRunning(programId.run(run21.getId()), runIdToSecs(run21));
    store.setSuspend(programId.run(run21.getId()), AppFabricTestHelper.createSourceId(++sourceId));

    // record not finished flow
    RunId run3 = RunIds.generate(now);
    setStartAndRunning(programId.run(run3.getId()), runIdToSecs(run3));

    // For a RunRecordMeta that has not yet been completed, getStopTs should return null
    RunRecordMeta runRecord = store.getRun(programId.run(run3.getId()));
    Assert.assertNotNull(runRecord);
    Assert.assertNull(runRecord.getStopTs());

    // record run of different program
    ProgramId programId2 = new ProgramId("account1", "application1", ProgramType.FLOW, "flow2");
    RunId run4 = RunIds.generate(now - 5000);
    setStartAndRunning(programId2.run(run4.getId()), runIdToSecs(run4));
    store.setStop(programId2.run(run4.getId()), startTimeSecs - 4, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    // record for different account
    setStartAndRunning(new ProgramId("account2", "application1", ProgramType.FLOW, "flow1").run(run3.getId()),
                       runIdToSecs(run3));

    // we should probably be better with "get" method in DefaultStore interface to do that, but we don't have one
    Map<ProgramRunId, RunRecordMeta> successHistorymap = store.getRuns(programId, ProgramRunStatus.COMPLETED,
                                                       0, Long.MAX_VALUE, Integer.MAX_VALUE);

    Map<ProgramRunId, RunRecordMeta> failureHistorymap = store.getRuns(programId, ProgramRunStatus.FAILED,
                                                                       startTimeSecs - 20, startTimeSecs - 10,
                                                                       Integer.MAX_VALUE);
    Assert.assertEquals(failureHistorymap, store.getRuns(programId, ProgramRunStatus.FAILED,
                                                      0, Long.MAX_VALUE, Integer.MAX_VALUE));

    Map<ProgramRunId, RunRecordMeta> suspendedHistorymap = store.getRuns(programId, ProgramRunStatus.SUSPENDED,
                                                                         startTimeSecs - 20, startTimeSecs,
                                                                         Integer.MAX_VALUE);

    // only finished + succeeded runs should be returned
    Assert.assertEquals(1, successHistorymap.size());
    // only finished + failed runs should be returned
    Assert.assertEquals(1, failureHistorymap.size());
    // only suspended runs should be returned
    Assert.assertEquals(1, suspendedHistorymap.size());

    // records should be sorted by start time latest to earliest
    RunRecordMeta run = successHistorymap.values().iterator().next();
    Assert.assertEquals(startTimeSecs - 10, run.getStartTs());
    Assert.assertEquals(Long.valueOf(startTimeSecs - 5), run.getStopTs());
    Assert.assertEquals(ProgramController.State.COMPLETED.getRunStatus(), run.getStatus());

    run = failureHistorymap.values().iterator().next();
    Assert.assertEquals(startTimeSecs - 20, run.getStartTs());
    Assert.assertEquals(Long.valueOf(startTimeSecs - 10), run.getStopTs());
    Assert.assertEquals(ProgramController.State.ERROR.getRunStatus(), run.getStatus());

    run = suspendedHistorymap.values().iterator().next();
    Assert.assertEquals(run21.getId(), run.getPid());
    Assert.assertEquals(ProgramController.State.SUSPENDED.getRunStatus(), run.getStatus());

    // Assert all history
    Map<ProgramRunId, RunRecordMeta> allHistorymap =
      store.getRuns(programId, ProgramRunStatus.ALL, startTimeSecs - 20, startTimeSecs + 1, Integer.MAX_VALUE);
    Assert.assertEquals(allHistorymap.toString(), 4, allHistorymap.size());

    // Assert running programs
    Map<ProgramRunId, RunRecordMeta> runningHistorymap = store.getRuns(programId, ProgramRunStatus.RUNNING,
                                                                       startTimeSecs, startTimeSecs + 1, 100);
    Assert.assertEquals(1, runningHistorymap.size());
    Assert.assertEquals(runningHistorymap, store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, 100));

    // Get a run record for running program
    RunRecordMeta expectedRunning = runningHistorymap.values().iterator().next();
    Assert.assertNotNull(expectedRunning);
    RunRecordMeta actualRunning = store.getRun(programId.run(expectedRunning.getPid()));
    Assert.assertEquals(expectedRunning, actualRunning);

    // Get a run record for completed run
    RunRecordMeta expectedCompleted = successHistorymap.values().iterator().next();
    Assert.assertNotNull(expectedCompleted);
    RunRecordMeta actualCompleted = store.getRun(programId.run(expectedCompleted.getPid()));
    Assert.assertEquals(expectedCompleted, actualCompleted);

    // Get a run record for suspended run
    RunRecordMeta expectedSuspended = suspendedHistorymap.values().iterator().next();
    Assert.assertNotNull(expectedSuspended);
    RunRecordMeta actualSuspended = store.getRun(programId.run(expectedSuspended.getPid()));
    Assert.assertEquals(expectedSuspended, actualSuspended);

    // Backwards compatibility test with random UUIDs
    // record finished flow
    RunId run5 = RunIds.fromString(UUID.randomUUID().toString());
    setStartAndRunning(programId.run(run5.getId()), startTimeSecs - 8);
    store.setStop(programId.run(run5.getId()), startTimeSecs - 4, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));
    ProgramRunCluster emptyCluster = new ProgramRunCluster(ProgramRunClusterStatus.PROVISIONED, null, 0);
    RunRecordMeta expectedRecord5 = RunRecordMeta.builder()
      .setProgramRunId(programId.run(run5))
      .setStartTime(startTimeSecs - 8)
      .setRunTime(startTimeSecs - 8 + startTimeDelaySecs)
      .setStopTime(startTimeSecs - 4)
      .setStatus(ProgramRunStatus.COMPLETED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();

    // record not finished flow
    RunId run6 = RunIds.fromString(UUID.randomUUID().toString());
    setStartAndRunning(programId.run(run6.getId()), startTimeSecs - 2);
    RunRecordMeta expectedRecord6 = RunRecordMeta.builder()
      .setProgramRunId(programId.run(run6))
      .setStartTime(startTimeSecs - 2)
      .setRunTime(startTimeSecs - 2 + startTimeDelaySecs)
      .setStatus(ProgramRunStatus.RUNNING)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();

    // Get run record for run5
    RunRecordMeta actualRecord5 = store.getRun(programId.run(run5.getId()));
    Assert.assertEquals(expectedRecord5, actualRecord5);

    // Get run record for run6

    RunRecordMeta actualRecord6 = store.getRun(programId.run(run6.getId()));
    Assert.assertEquals(expectedRecord6, actualRecord6);

    // Record flow that starts but encounters error before it runs
    RunId run7 = RunIds.fromString(UUID.randomUUID().toString());
    Map<String, String> emptyArgs = ImmutableMap.of();
    setStart(programId.run(run7.getId()), startTimeSecs, emptyArgs, emptyArgs);
    store.setStop(programId.run(run7.getId()), startTimeSecs + 1, ProgramController.State.ERROR.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));
    RunRecordMeta expectedRunRecord7 = RunRecordMeta.builder()
      .setProgramRunId(programId.run(run7))
      .setStartTime(startTimeSecs)
      .setStopTime(startTimeSecs + 1)
      .setStatus(ProgramRunStatus.FAILED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();
    RunRecordMeta actualRecord7 = store.getRun(programId.run(run7.getId()));
    Assert.assertEquals(expectedRunRecord7, actualRecord7);

    // Record flow that starts and suspends before it runs
    RunId run8 = RunIds.fromString(UUID.randomUUID().toString());
    setStart(programId.run(run8.getId()), startTimeSecs, emptyArgs, emptyArgs);
    store.setSuspend(programId.run(run8.getId()), AppFabricTestHelper.createSourceId(++sourceId));
    RunRecordMeta expectedRunRecord8 = RunRecordMeta.builder()
      .setProgramRunId(programId.run(run8))
      .setStartTime(startTimeSecs)
      .setStatus(ProgramRunStatus.SUSPENDED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();
    RunRecordMeta actualRecord8 = store.getRun(programId.run(run8.getId()));
    Assert.assertEquals(expectedRunRecord8, actualRecord8);

    // Record flow that is killed while suspended
    RunId run9 = RunIds.fromString(UUID.randomUUID().toString());
    setStartAndRunning(programId.run(run9.getId()), startTimeSecs);
    store.setSuspend(programId.run(run9.getId()), AppFabricTestHelper.createSourceId(++sourceId));
    store.setStop(programId.run(run9.getId()), startTimeSecs + 5, ProgramRunStatus.KILLED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    RunRecordMeta expectedRunRecord9 = RunRecordMeta.builder()
      .setProgramRunId(programId.run(run9))
      .setStartTime(startTimeSecs)
      .setRunTime(startTimeSecs + 1)
      .setStopTime(startTimeSecs + 5)
      .setStatus(ProgramRunStatus.KILLED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();
    RunRecordMeta actualRecord9 = store.getRun(programId.run(run9.getId()));
    Assert.assertEquals(expectedRunRecord9, actualRecord9);

    // Non-existent run record should give null
    Assert.assertNull(store.getRun(programId.run(UUID.randomUUID().toString())));

    // Searching for history in wrong time range should give us no results
    Assert.assertTrue(
      store.getRuns(programId, ProgramRunStatus.COMPLETED, startTimeSecs - 5000, startTimeSecs - 2000,
                    Integer.MAX_VALUE).isEmpty()
    );
    Assert.assertTrue(
      store.getRuns(programId, ProgramRunStatus.ALL, startTimeSecs - 5000, startTimeSecs - 2000,
                    Integer.MAX_VALUE).isEmpty()
    );
  }

  private long runIdToSecs(RunId runId) {
    return RunIds.getTime(runId, TimeUnit.SECONDS);
  }

  @Test
  public void testAddApplication() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    ApplicationId appId = new ApplicationId("account1", "application1");
    store.addApplication(appId, spec);

    ApplicationSpecification stored = store.getApplication(appId);
    assertWordCountAppSpecAndInMetadataStore(stored);
  }

  @Test
  public void testUpdateChangedApplication() throws Exception {
    ApplicationId id = new ApplicationId("account1", "application1");

    store.addApplication(id, Specifications.from(new FooApp()));
    // update
    store.addApplication(id, Specifications.from(new ChangedFooApp()));

    ApplicationSpecification stored = store.getApplication(id);
    assertChangedFooAppSpecAndInMetadataStore(stored);
  }

  private static class FooApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("FooApp");
      setDescription("Foo App");
      addStream(new Stream("stream1"));
      addStream(new Stream("stream2"));
      createDataset("dataset1", Table.class);
      createDataset("dataset2", KeyValueTable.class);
      addFlow(new FlowImpl("flow1"));
      addFlow(new FlowImpl("flow2"));
      addMapReduce(new FooMapReduceJob("mrJob1"));
      addMapReduce(new FooMapReduceJob("mrJob2"));
    }
  }

  private static class ChangedFooApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("FooApp");
      setDescription("Foo App");
      addStream(new Stream("stream2"));
      addStream(new Stream("stream3"));
      createDataset("dataset2", KeyValueTable.class);

      createDataset("dataset3", IndexedTable.class,
                    DatasetProperties.builder().add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "foo").build());
      addFlow(new FlowImpl("flow2"));
      addFlow(new FlowImpl("flow3"));
      addMapReduce(new FooMapReduceJob("mrJob2"));
      addMapReduce(new FooMapReduceJob("mrJob3"));
    }
  }

  private static class FlowImpl extends AbstractFlow {
    private String name;

    private FlowImpl(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      setName(name);
      setDescription("Flow for counting words");
      addFlowlet(new FlowletImpl("flowlet1"));
      connectStream("stream1", new FlowletImpl("flowlet1"));
    }
  }

  /**
   *
   */
  public static class FlowletImpl extends AbstractFlowlet {
    private final String name;

    @UseDataSet("dataset2")
    @SuppressWarnings("unused")
    private KeyValueTable counters;

    @Output("output")
    @SuppressWarnings("unused")
    private OutputEmitter<String> output;

    protected FlowletImpl(String name) {
      this.name = name;
    }

    @ProcessInput("process")
    public void bar(String str) {
      output.emit(str);
    }

    @Override
    protected void configure() {
      setName(name);
    }
  }

  /**
   * Map reduce job for testing MDS.
   */
  public static class FooMapReduceJob extends AbstractMapReduce {
    private final String name;

    public FooMapReduceJob(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing MDS");
    }
  }

  private void assertWordCountAppSpecAndInMetadataStore(ApplicationSpecification stored) {
    // should be enough to make sure it is stored
    Assert.assertEquals(WordCountApp.WordCountFlow.class.getName(),
                        stored.getFlows().get("WordCountFlow").getClassName());
  }

  private void assertChangedFooAppSpecAndInMetadataStore(ApplicationSpecification stored) {
    // should be enough to make sure it is stored
    Assert.assertEquals(FlowImpl.class.getName(),
                        stored.getFlows().get("flow2").getClassName());
  }


  @Test
  public void testServiceDeletion() throws Exception {
    // Store the application specification
    AbstractApplication app = new AppWithServices();

    ApplicationSpecification appSpec = Specifications.from(app);
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    store.addApplication(appId, appSpec);

    AbstractApplication newApp = new AppWithNoServices();

    // get the delete program specs after deploying AppWithNoServices
    List<ProgramSpecification> programSpecs = store.getDeletedProgramSpecifications(appId, Specifications.from(newApp));

    //verify the result.
    Assert.assertEquals(1, programSpecs.size());
    Assert.assertEquals("NoOpService", programSpecs.get(0).getName());
  }

  @Test
  public void testServiceInstances() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new AppWithServices());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    store.addApplication(appId, appSpec);

    // Test setting of service instances
    ProgramId programId = appId.program(ProgramType.SERVICE, "NoOpService");
    int count = store.getServiceInstances(programId);
    Assert.assertEquals(1, count);

    store.setServiceInstances(programId, 10);
    count = store.getServiceInstances(programId);
    Assert.assertEquals(10, count);

    ApplicationSpecification newSpec = store.getApplication(appId);
    Assert.assertNotNull(newSpec);
    Map<String, ServiceSpecification> services = newSpec.getServices();
    Assert.assertEquals(1, services.size());

    ServiceSpecification serviceSpec = services.get("NoOpService");
    Assert.assertEquals(10, serviceSpec.getInstances());
  }

  @Test
  public void testSetFlowletInstances() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    int initialInstances = spec.getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances();
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    store.addApplication(appId, spec);

    ProgramId programId = appId.flow("WordCountFlow");
    store.setFlowletInstances(programId, "StreamSource",
                              initialInstances + 5);
    // checking that app spec in store was adjusted
    ApplicationSpecification adjustedSpec = store.getApplication(appId);
    Assert.assertNotNull(adjustedSpec);
    Assert.assertEquals(initialInstances + 5,
                        adjustedSpec.getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances());

    // checking that program spec in program jar was adjusted
    ProgramDescriptor descriptor = store.loadProgram(programId);
    Assert.assertNotNull(descriptor);
    Assert.assertEquals(initialInstances + 5,
                        descriptor.getApplicationSpecification().
                          getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances());
  }

  @Test
  public void testWorkerInstances() throws Exception {
    ApplicationSpecification spec = Specifications.from(new AppWithWorker());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    store.addApplication(appId, spec);

    ProgramId programId = appId.worker(AppWithWorker.WORKER);
    int instancesFromSpec = spec.getWorkers().get(AppWithWorker.WORKER).getInstances();
    Assert.assertEquals(1, instancesFromSpec);
    int instances = store.getWorkerInstances(programId);
    Assert.assertEquals(instancesFromSpec, instances);

    store.setWorkerInstances(programId, 9);
    instances = store.getWorkerInstances(programId);
    Assert.assertEquals(9, instances);
  }

  @Test
  public void testRemoveAllApplications() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app(spec.getName());
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    // removing flow
    store.removeAllApplications(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRemoveAll() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app("application1");
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    // removing flow
    store.removeAll(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRemoveApplication() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app(spec.getName());
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRuntimeArgsDeletion() throws Exception {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = new ApplicationId("testDeleteRuntimeArgs", spec.getName());
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    ProgramId flowProgramId = appId.flow("NoOpFlow");
    ProgramId mapreduceProgramId = appId.mr("NoOpMR");
    ProgramId workflowProgramId = appId.workflow("NoOpWorkflow");

    String flowRunId = RunIds.generate().getId();
    String mapreduceRunId = RunIds.generate().getId();
    String workflowRunId = RunIds.generate().getId();

    ProgramRunId flowProgramRunId = flowProgramId.run(flowRunId);
    ProgramRunId mapreduceProgramRunId = mapreduceProgramId.run(mapreduceRunId);
    ProgramRunId workflowProgramRunId = workflowProgramId.run(workflowRunId);

    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    setStartAndRunning(flowProgramId.run(flowRunId), nowSecs,
                       ImmutableMap.of("model", "click"), Collections.emptyMap());
    setStartAndRunning(mapreduceProgramId.run(mapreduceRunId), nowSecs,
                       ImmutableMap.of("path", "/data"), Collections.emptyMap());
    setStartAndRunning(workflowProgramId.run(workflowRunId), nowSecs,
                       ImmutableMap.of("whitelist", "cask"), Collections.emptyMap());

    Map<String, String> args = store.getRuntimeArguments(flowProgramRunId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("click", args.get("model"));

    args = store.getRuntimeArguments(mapreduceProgramRunId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("/data", args.get("path"));

    args = store.getRuntimeArguments(workflowProgramRunId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("cask", args.get("whitelist"));

    // removing application
    store.removeApplication(appId);

    //Check if args are deleted.
    args = store.getRuntimeArguments(flowProgramRunId);
    Assert.assertEquals(0, args.size());

    args = store.getRuntimeArguments(mapreduceProgramRunId);
    Assert.assertEquals(0, args.size());

    args = store.getRuntimeArguments(workflowProgramRunId);
    Assert.assertEquals(0, args.size());
  }

  @Test
  public void testHistoryDeletion() throws Exception {

    // Deploy two apps, write some history for programs
    // Remove application using accountId, AppId and verify
    // Remove all from accountId and verify
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("testDeleteAll");
    ApplicationId appId1 = namespaceId.app(spec.getName());
    store.addApplication(appId1, spec);

    spec = Specifications.from(new WordCountApp());
    ApplicationId appId2 = namespaceId.app(spec.getName());
    store.addApplication(appId2, spec);

    ProgramId flowProgramId1 = appId1.flow("NoOpFlow");
    ProgramId mapreduceProgramId1 = appId1.mr("NoOpMR");
    ProgramId workflowProgramId1 = appId1.workflow("NoOpWorkflow");

    ProgramId flowProgramId2 = appId2.flow("WordCountFlow");

    Assert.assertNotNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    long now = System.currentTimeMillis();

    ProgramRunId flowProgramRunId1 = flowProgramId1.run(RunIds.generate());
    setStartAndRunning(flowProgramRunId1, now - 1000);
    store.setStop(flowProgramRunId1, now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    ProgramRunId mapreduceProgramRunId1 = mapreduceProgramId1.run(RunIds.generate());
    setStartAndRunning(mapreduceProgramRunId1, now - 1000);
    store.setStop(mapreduceProgramRunId1, now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    RunId runId = RunIds.generate(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(1000));
    setStartAndRunning(workflowProgramId1.run(runId.getId()), now - 1000);
    store.setStop(workflowProgramId1.run(runId.getId()), now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    ProgramRunId flowProgramRunId2 = flowProgramId2.run(RunIds.generate());
    setStartAndRunning(flowProgramRunId2, now - 1000);
    store.setStop(flowProgramRunId2, now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    verifyRunHistory(flowProgramId1, 1);
    verifyRunHistory(mapreduceProgramId1, 1);
    verifyRunHistory(workflowProgramId1, 1);

    verifyRunHistory(flowProgramId2, 1);

    // removing application
    store.removeApplication(appId1);

    Assert.assertNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    verifyRunHistory(flowProgramId1, 0);
    verifyRunHistory(mapreduceProgramId1, 0);
    verifyRunHistory(workflowProgramId1, 0);

    // Check to see if the flow history of second app is not deleted
    verifyRunHistory(flowProgramId2, 1);

    // remove all
    store.removeAll(namespaceId);
    
    verifyRunHistory(flowProgramId2, 0);
  }

  private void verifyRunHistory(ProgramId programId, int count) {
    Map<ProgramRunId, RunRecordMeta> historymap = store.getRuns(programId, ProgramRunStatus.ALL,
                                                0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(count, historymap.size());
  }

  @Test
  public void testRunsLimit() throws Exception {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = new ApplicationId("testRunsLimit", spec.getName());
    store.addApplication(appId, spec);

    ProgramId flowProgramId = new ProgramId("testRunsLimit", spec.getName(), ProgramType.FLOW, "NoOpFlow");

    Assert.assertNotNull(store.getApplication(appId));

    long now = System.currentTimeMillis();
    ProgramRunId flowProgramRunId = flowProgramId.run(RunIds.generate());
    setStartAndRunning(flowProgramRunId, now - 3000);
    store.setStop(flowProgramRunId, now - 100, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    setStartAndRunning(flowProgramId.run(RunIds.generate()), now - 2000);

    // even though there's two separate run records (one that's complete and one that's active), only one should be
    // returned by the query, because the limit parameter of 1 is being passed in.
    Map<ProgramRunId, RunRecordMeta> historymap = store.getRuns(flowProgramId, ProgramRunStatus.ALL,
                                                                0, Long.MAX_VALUE, 1);
    Assert.assertEquals(1, historymap.size());
  }

  @Test
  public void testCheckDeletedProgramSpecs() throws Exception {
    //Deploy program with all types of programs.
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    store.addApplication(appId, spec);

    Set<String> specsToBeVerified = Sets.newHashSet();
    specsToBeVerified.addAll(spec.getMapReduce().keySet());
    specsToBeVerified.addAll(spec.getWorkflows().keySet());
    specsToBeVerified.addAll(spec.getFlows().keySet());
    specsToBeVerified.addAll(spec.getServices().keySet());
    specsToBeVerified.addAll(spec.getWorkers().keySet());
    specsToBeVerified.addAll(spec.getSpark().keySet());

    //Verify if there are 6 program specs in AllProgramsApp
    Assert.assertEquals(7, specsToBeVerified.size());

    // Check the diff with the same app - re-deployment scenario where programs are not removed.
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(0, deletedSpecs.size());

    //Get the spec for app that contains no programs.
    spec = Specifications.from(new NoProgramsApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(7, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeVerified.remove(specification.getName());
    }

    //All the 6 specs should have been deleted.
    Assert.assertEquals(0, specsToBeVerified.size());
  }

  @Test
  public void testCheckDeletedWorkflow() throws Exception {
    //Deploy program with all types of programs.
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    store.addApplication(appId, spec);

    Set<String> specsToBeDeleted = Sets.newHashSet();
    specsToBeDeleted.addAll(spec.getWorkflows().keySet());

    Assert.assertEquals(1, specsToBeDeleted.size());

    //Get the spec for app that contains only flow and mapreduce - removing workflows.
    spec = Specifications.from(new FlowMapReduceApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(2, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeDeleted.remove(specification.getName());
    }

    //2 specs should have been deleted and 0 should be remaining.
    Assert.assertEquals(0, specsToBeDeleted.size());
  }

  @Test
  public void testRunningInRangeSimple() throws Exception {
    NamespaceId ns = new NamespaceId("d");
    ProgramRunId run1 = ns.app("a1").program(ProgramType.FLOW, "f1").run(RunIds.generate(20000).getId());
    ProgramRunId run2 = ns.app("a2").program(ProgramType.MAPREDUCE, "f2").run(RunIds.generate(10000).getId());
    ProgramRunId run3 = ns.app("a3").program(ProgramType.WORKER, "f3").run(RunIds.generate(40000).getId());
    ProgramRunId run4 = ns.app("a4").program(ProgramType.SERVICE, "f4").run(RunIds.generate(70000).getId());
    ProgramRunId run5 = ns.app("a5").program(ProgramType.SPARK, "f5").run(RunIds.generate(30000).getId());
    ProgramRunId run6 = ns.app("a6").program(ProgramType.WORKFLOW, "f6").run(RunIds.generate(60000).getId());

    writeStartRecord(run1);
    writeStartRecord(run2);
    writeStartRecord(run3);
    writeStartRecord(run4);
    writeStartRecord(run5);
    writeStartRecord(run6);

    Assert.assertEquals(runsToTime(run1, run2), runIdsToTime(store.getRunningInRange(1, 30)));
    Assert.assertEquals(runsToTime(run1, run2, run5, run3), runIdsToTime(store.getRunningInRange(30, 50)));
    Assert.assertEquals(runsToTime(run1, run2, run3, run4, run5, run6),
                        runIdsToTime(store.getRunningInRange(1, 71)));
    Assert.assertEquals(runsToTime(run1, run2, run3, run4, run5, run6),
                        runIdsToTime(store.getRunningInRange(50, 71)));
    Assert.assertEquals(ImmutableSet.of(), runIdsToTime(store.getRunningInRange(1, 10)));

    writeStopRecord(run1, 45000);
    writeStopRecord(run3, 55000);
    writeSuspendedRecord(run5);

    Assert.assertEquals(runsToTime(run2, run3, run4, run5, run6),
                        runIdsToTime(store.getRunningInRange(50, 71)));
  }

  @SuppressWarnings("PointlessArithmeticExpression")
  @Test
  public void testRunningInRangeMulti() throws Exception {
    // Add some run records
    TreeSet<Long> allPrograms = new TreeSet<>();
    TreeSet<Long> stoppedPrograms = new TreeSet<>();
    TreeSet<Long> suspendedPrograms = new TreeSet<>();
    TreeSet<Long> runningPrograms = new TreeSet<>();
    for (int i = 0; i < 99; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app" + i);
      ProgramId program = application.program(ProgramType.values()[i % ProgramType.values().length],
                                              "program" + i);
      long startTime = (i + 1) * 10000;
      RunId runId = RunIds.generate(startTime);
      allPrograms.add(startTime);
      ProgramRunId run = program.run(runId.getId());
      writeStartRecord(run);

      // For every 3rd program starting from 0th, write stop record
      if ((i % 3) == 0) {
        writeStopRecord(run, startTime + 10);
        stoppedPrograms.add(startTime);
      }

      // For every 3rd program starting from 1st, write suspended record
      if ((i % 3) == 1) {
        writeSuspendedRecord(run);
        suspendedPrograms.add(startTime);
      }

      // The rest are running programs
      if ((i % 3) == 2) {
        runningPrograms.add(startTime);
      }
    }

    // In all below assertions, TreeSet and metadataStore both have start time inclusive and end time exclusive.
    // querying full range should give all programs
    Assert.assertEquals(allPrograms, runIdsToTime(store.getRunningInRange(0, Long.MAX_VALUE)));
    Assert.assertEquals(allPrograms, runIdsToTime(store.getRunningInRange(1 * 10, 100 * 10)));

    // querying a range before start time of first program should give empty results
    Assert.assertEquals(ImmutableSet.of(), runIdsToTime(store.getRunningInRange(1, 1 * 10)));

    // querying a range after the stop time of the last program should return only running and suspended programs
    Assert.assertEquals(ImmutableSortedSet.copyOf(Iterables.concat(suspendedPrograms, runningPrograms)),
                        runIdsToTime(store.getRunningInRange(100 * 10, Long.MAX_VALUE)));

    // querying a range completely within the start time of the first program and stop time of the last program
    // should give all running and suspended programs started after given start time,
    // and all stopped programs between given start and stop time.
    Assert.assertEquals(ImmutableSortedSet.copyOf(
                          Iterables.concat(stoppedPrograms.subSet(30 * 10000L, 60 * 10000L),
                                           suspendedPrograms.subSet(1 * 10000L, 60 * 10000L),
                                           runningPrograms.subSet(1 * 10000L, 60 * 10000L))),
                        runIdsToTime(store.getRunningInRange(30 * 10, 60 * 10)));

    // querying a range after start time of first program to after the stop time of the last program
    // should give all running and suspended programs after start time of first program
    // and all stopped programs after the given start time
    Assert.assertEquals(ImmutableSortedSet.copyOf(
                          Iterables.concat(stoppedPrograms.subSet(30 * 10000L, 150 * 10000L),
                                           suspendedPrograms.subSet(1 * 10000L, 150 * 10000L),
                                           runningPrograms.subSet(1 * 10000L, 150 * 10000L))),
                        runIdsToTime(store.getRunningInRange(30 * 10, 150 * 10)));

    // querying a range before start time of first program to before the stop time of the last program
    // should give all running, suspended, and stopped programs from the first program to the given stop time
    Assert.assertEquals(ImmutableSortedSet.copyOf(
                          Iterables.concat(stoppedPrograms.subSet(1000L, 45 * 10000L),
                                           suspendedPrograms.subSet(1000L, 45 * 10000L),
                                           runningPrograms.subSet(1000L, 45 * 10000L))),
                        runIdsToTime(store.getRunningInRange(1, 45 * 10)));
  }

  private void writeStartRecord(ProgramRunId run) {
    long startTimeSecs = RunIds.getTime(RunIds.fromString(run.getRun()), TimeUnit.SECONDS);
    setStartAndRunning(run, startTimeSecs);
    Assert.assertNotNull(store.getRun(run));
  }

  private void writeStopRecord(ProgramRunId run, long stopTimeInMillis) {
    store.setStop(run, TimeUnit.MILLISECONDS.toSeconds(stopTimeInMillis),
                  ProgramRunStatus.COMPLETED, AppFabricTestHelper.createSourceId(++sourceId));
    Assert.assertNotNull(store.getRun(run));
  }

  private void writeSuspendedRecord(ProgramRunId run) {
    store.setSuspend(run, AppFabricTestHelper.createSourceId(++sourceId));
    Assert.assertNotNull(store.getRun(run));
  }

  private Set<Long> runsToTime(ProgramRunId... runIds) {
    Iterable<Long> transformedRunIds = Iterables.transform(ImmutableSet.copyOf(runIds), runId ->
      RunIds.getTime(RunIds.fromString(runId.getRun()), TimeUnit.MILLISECONDS));
    return ImmutableSortedSet.copyOf(transformedRunIds);
  }

  private SortedSet<Long> runIdsToTime(Set<RunId> runIds) {
    Iterable<Long> transformedRunIds = Iterables.transform(runIds,
                                                           runId -> RunIds.getTime(runId, TimeUnit.MILLISECONDS));
    return ImmutableSortedSet.copyOf(transformedRunIds);
  }
}
