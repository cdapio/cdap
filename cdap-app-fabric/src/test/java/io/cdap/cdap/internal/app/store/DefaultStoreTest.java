/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithNoServices;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.AppWithWorker;
import io.cdap.cdap.DefaultStoreTestApp;
import io.cdap.cdap.NoProgramsApp;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.state.AppStateKey;
import io.cdap.cdap.internal.app.store.state.AppStateKeyValue;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.store.DefaultNamespaceStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.twill.api.RunId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DefaultStore}.
 */
public abstract class DefaultStoreTest {
  protected static DefaultStore store;
  protected static DefaultNamespaceStore nsStore;
  protected static NamespaceAdmin nsAdmin;
  private static final Gson GSON = new Gson();

  private int sourceId;

  @Before
  public void before() throws Exception {
    store.clear();
    nsStore.delete(new NamespaceId("default"));
    NamespacePathLocator namespacePathLocator =
      AppFabricTestHelper.getInjector().getInstance(NamespacePathLocator.class);
    namespacePathLocator.get(NamespaceId.DEFAULT).delete(true);
    nsAdmin.create(NamespaceMeta.DEFAULT);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  private void setStartAndRunning(ProgramRunId id, ArtifactId artifactId) {
    setStartAndRunning(id, ImmutableMap.of(), ImmutableMap.of(), artifactId);
  }

  private void setStartAndRunning(ProgramRunId id, Map<String, String> runtimeArgs, Map<String, String> systemArgs,
                                  ArtifactId artifactId) {
    long startTime = RunIds.getTime(id.getRun(), TimeUnit.SECONDS);
    setStart(id, runtimeArgs, systemArgs, artifactId);
    store.setRunning(id, startTime + 1, null, AppFabricTestHelper.createSourceId(++sourceId));
  }

  private void setStart(ProgramRunId id, Map<String, String> runtimeArgs, Map<String, String> systemArgs,
                        ArtifactId artifactId) {
    if (!systemArgs.containsKey(SystemArguments.PROFILE_NAME)) {
      systemArgs = ImmutableMap.<String, String>builder()
        .putAll(systemArgs)
        .put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName())
        .build();
    }
    store.setProvisioning(id, runtimeArgs, systemArgs, AppFabricTestHelper.createSourceId(++sourceId), artifactId);
    store.setProvisioned(id, 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(id, null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));
  }

  @Test
  public void testLoadingProgram() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new FooApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(appSpec.getName(), appSpec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    ProgramDescriptor descriptor = store.loadProgram(appId.mr("mrJob1"));
    Assert.assertNotNull(descriptor);
    MapReduceSpecification mrSpec = descriptor.getSpecification();
    Assert.assertEquals("mrJob1", mrSpec.getName());
    Assert.assertEquals(FooMapReduceJob.class.getName(), mrSpec.getClassName());
  }

  @Test
  public void testStopBeforeStart() throws RuntimeException {
    ProgramId programId = new ProgramId("account1", "invalidApp", ProgramType.MAPREDUCE, "InvalidMR");
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
    ArtifactId artifactId = namespaceId.artifact("testArtifact", "1.0").toApiArtifactId();
    RunId run1 = RunIds.generate();
    setStartAndRunning(programId1.run(run1.getId()), artifactId);
    store.setSuspend(programId1.run(run1.getId()), AppFabricTestHelper.createSourceId(++sourceId), -1);
    store.removeApplication(appId1);
    Assert.assertTrue(store.getRuns(programId1, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).isEmpty());

    // Test delete namespace
    ProgramId programId2 = namespaceId.app("app2").workflow("pgm2");
    RunId run2 = RunIds.generate();
    setStartAndRunning(programId2.run(run2.getId()), artifactId);
    store.setSuspend(programId2.run(run2.getId()), AppFabricTestHelper.createSourceId(++sourceId), -1);
    store.removeAll(namespaceId);
    nsStore.delete(namespaceId);
    Assert.assertTrue(store.getRuns(programId2, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).isEmpty());
  }

  @Test
  public void testWorkflowNodeState() {
    String namespaceName = "namespace1";
    String appName = "app1";
    String workflowName = "workflow1";
    String mapReduceName = "mapReduce1";

    ApplicationId appId = Ids.namespace(namespaceName).app(appName);

    long currentTime = System.currentTimeMillis();
    String workflowRunId = RunIds.generate(currentTime).getId();
    ProgramRunId workflowRun = appId.workflow(workflowName).run(workflowRunId);
    ArtifactId artifactId = appId.getParent().artifact("testArtifact", "1.0").toApiArtifactId();

    // start Workflow
    setStartAndRunning(workflowRun, artifactId);

    // start MapReduce as a part of Workflow
    Map<String, String> systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, mapReduceName,
                                                     ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                                     ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);

    RunId mapReduceRunId = RunIds.generate(currentTime + 10);
    ProgramId mapReduceProgram = appId.mr(mapReduceName);

    setStartAndRunning(mapReduceProgram.run(mapReduceRunId.getId()), ImmutableMap.of(), systemArgs, artifactId);

    // stop the MapReduce program
    store.setStop(mapReduceProgram.run(mapReduceRunId.getId()), currentTime + 50, ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    // start Spark program as a part of Workflow
    String sparkName = "spark1";
    systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, sparkName,
                                 ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                 ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);

    RunId sparkRunId = RunIds.generate(currentTime + 60);
    ProgramId sparkProgram = appId.spark(sparkName);

    setStartAndRunning(sparkProgram.run(sparkRunId.getId()), ImmutableMap.of(), systemArgs, artifactId);

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
    Assert.assertEquals("illegal argument", failureCause.getMessage());
    Assert.assertEquals(IllegalArgumentException.class.getName(), failureCause.getClassName());
    failureCause = failureCause.getCause();
    Assert.assertNotNull(failureCause);
    Assert.assertEquals("dataset not found", failureCause.getMessage());
    Assert.assertEquals(NullPointerException.class.getName(), failureCause.getClassName());
    Assert.assertNull(failureCause.getCause());
  }

  @Test
  public void testConcurrentStopStart() {
    // Two programs that start/stop at same time
    // Should have two run history.
    ProgramId programId = new ProgramId("account1", "concurrentApp", ProgramType.MAPREDUCE, "concurrentMR");
    long now = System.currentTimeMillis();
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);

    RunId run1 = RunIds.generate(now - 10000);
    ArtifactId artifactId = programId.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(programId.run(run1.getId()), artifactId);
    RunId run2 = RunIds.generate(now - 10000);
    setStartAndRunning(programId.run(run2.getId()), artifactId);

    store.setStop(programId.run(run1.getId()), nowSecs, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));
    store.setStop(programId.run(run2.getId()), nowSecs, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    Map<ProgramRunId, RunRecordDetail> historymap = store.getRuns(programId, ProgramRunStatus.ALL,
                                                                  0, Long.MAX_VALUE, Integer.MAX_VALUE);

    Assert.assertEquals(2, historymap.size());
  }

  @Test
  public void testLogProgramRunHistory() {
    // record finished Workflow
    ProgramId programId = new ProgramId("account1", "application1", ProgramType.WORKFLOW, "wf1");
    long now = System.currentTimeMillis();
    long startTimeSecs = TimeUnit.MILLISECONDS.toSeconds(now);

    RunId run1 = RunIds.generate(now - 20000);
    ArtifactId artifactId = programId.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
    setStartAndRunning(programId.run(run1.getId()), artifactId);
    store.setStop(programId.run(run1.getId()), startTimeSecs - 10, ProgramController.State.ERROR.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    // record another finished Workflow
    RunId run2 = RunIds.generate(now - 10000);
    setStartAndRunning(programId.run(run2.getId()), artifactId);
    store.setStop(programId.run(run2.getId()), startTimeSecs - 5, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    // record a suspended Workflow
    RunId run21 = RunIds.generate(now - 7500);
    setStartAndRunning(programId.run(run21.getId()), artifactId);
    store.setSuspend(programId.run(run21.getId()), AppFabricTestHelper.createSourceId(++sourceId), -1);

    // record not finished Workflow
    RunId run3 = RunIds.generate(now);
    setStartAndRunning(programId.run(run3.getId()), artifactId);

    // For a RunRecordDetail that has not yet been completed, getStopTs should return null
    RunRecordDetail runRecord = store.getRun(programId.run(run3.getId()));
    Assert.assertNotNull(runRecord);
    Assert.assertNull(runRecord.getStopTs());

    // record run of different program
    ProgramId programId2 = new ProgramId("account1", "application1", ProgramType.WORKFLOW, "wf2");
    RunId run4 = RunIds.generate(now - 5000);
    setStartAndRunning(programId2.run(run4.getId()), artifactId);
    store.setStop(programId2.run(run4.getId()), startTimeSecs - 4, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    // record for different account
    setStartAndRunning(new ProgramId("account2", "application1", ProgramType.WORKFLOW, "wf1").run(run3.getId()),
                       artifactId);

    // we should probably be better with "get" method in DefaultStore interface to do that, but we don't have one
    Map<ProgramRunId, RunRecordDetail> successHistorymap = store.getRuns(programId, ProgramRunStatus.COMPLETED,
                                                                         0, Long.MAX_VALUE, Integer.MAX_VALUE);

    Map<ProgramRunId, RunRecordDetail> failureHistorymap = store.getRuns(programId, ProgramRunStatus.FAILED,
                                                                         startTimeSecs - 20, startTimeSecs - 10,
                                                                         Integer.MAX_VALUE);
    Assert.assertEquals(failureHistorymap, store.getRuns(programId, ProgramRunStatus.FAILED,
                                                         0, Long.MAX_VALUE, Integer.MAX_VALUE));

    Map<ProgramRunId, RunRecordDetail> suspendedHistorymap = store.getRuns(programId, ProgramRunStatus.SUSPENDED,
                                                                           startTimeSecs - 20, startTimeSecs,
                                                                           Integer.MAX_VALUE);

    // only finished + succeeded runs should be returned
    Assert.assertEquals(1, successHistorymap.size());
    // only finished + failed runs should be returned
    Assert.assertEquals(1, failureHistorymap.size());
    // only suspended runs should be returned
    Assert.assertEquals(1, suspendedHistorymap.size());

    // records should be sorted by start time latest to earliest
    RunRecordDetail run = successHistorymap.values().iterator().next();
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
    Map<ProgramRunId, RunRecordDetail> allHistorymap =
      store.getRuns(programId, ProgramRunStatus.ALL, startTimeSecs - 20, startTimeSecs + 1, Integer.MAX_VALUE);
    Assert.assertEquals(allHistorymap.toString(), 4, allHistorymap.size());

    // Assert running programs
    Map<ProgramRunId, RunRecordDetail> runningHistorymap = store.getRuns(programId, ProgramRunStatus.RUNNING,
                                                                         startTimeSecs, startTimeSecs + 1, 100);
    Assert.assertEquals(1, runningHistorymap.size());
    Assert.assertEquals(runningHistorymap, store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, 100));

    // Get a run record for running program
    RunRecordDetail expectedRunning = runningHistorymap.values().iterator().next();
    Assert.assertNotNull(expectedRunning);
    RunRecordDetail actualRunning = store.getRun(programId.run(expectedRunning.getPid()));
    Assert.assertEquals(expectedRunning, actualRunning);

    // Get a run record for completed run
    RunRecordDetail expectedCompleted = successHistorymap.values().iterator().next();
    Assert.assertNotNull(expectedCompleted);
    RunRecordDetail actualCompleted = store.getRun(programId.run(expectedCompleted.getPid()));
    Assert.assertEquals(expectedCompleted, actualCompleted);

    // Get a run record for suspended run
    RunRecordDetail expectedSuspended = suspendedHistorymap.values().iterator().next();
    Assert.assertNotNull(expectedSuspended);
    RunRecordDetail actualSuspended = store.getRun(programId.run(expectedSuspended.getPid()));
    Assert.assertEquals(expectedSuspended, actualSuspended);

    ProgramRunCluster emptyCluster = new ProgramRunCluster(ProgramRunClusterStatus.PROVISIONED, null, 0);
    // Record workflow that starts but encounters error before it runs
    RunId run7 = RunIds.generate(now);
    Map<String, String> emptyArgs = ImmutableMap.of();
    setStart(programId.run(run7.getId()), emptyArgs, emptyArgs, artifactId);
    store.setStop(programId.run(run7.getId()), startTimeSecs + 1, ProgramController.State.ERROR.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));
    Map<String, String> noRuntimeArgsProps = ImmutableMap.of("runtimeArgs",
        GSON.toJson(ImmutableMap.<String, String>of()));

    RunRecordDetail expectedRunRecord7 = RunRecordDetail.builder()
      .setProgramRunId(programId.run(run7))
      .setStartTime(startTimeSecs)
      .setStopTime(startTimeSecs + 1)
      .setStatus(ProgramRunStatus.FAILED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setArtifactId(artifactId)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();
    RunRecordDetail actualRecord7 = store.getRun(programId.run(run7.getId()));
    Assert.assertEquals(expectedRunRecord7, actualRecord7);

    // Record workflow that starts and suspends before it runs
    RunId run8 = RunIds.generate(now);
    setStart(programId.run(run8.getId()), emptyArgs, emptyArgs, artifactId);
    store.setSuspend(programId.run(run8.getId()), AppFabricTestHelper.createSourceId(++sourceId), -1);
    RunRecordDetail expectedRunRecord8 = RunRecordDetail.builder()
      .setProgramRunId(programId.run(run8))
      .setStartTime(startTimeSecs)
      .setStatus(ProgramRunStatus.SUSPENDED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setArtifactId(artifactId)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();
    RunRecordDetail actualRecord8 = store.getRun(programId.run(run8.getId()));
    Assert.assertEquals(expectedRunRecord8, actualRecord8);

    // Record workflow that is killed while suspended
    RunId run9 = RunIds.generate(now);
    setStartAndRunning(programId.run(run9.getId()), artifactId);
    store.setSuspend(programId.run(run9.getId()), AppFabricTestHelper.createSourceId(++sourceId), -1);
    store.setStop(programId.run(run9.getId()), startTimeSecs + 5, ProgramRunStatus.KILLED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    RunRecordDetail expectedRunRecord9 = RunRecordDetail.builder()
      .setProgramRunId(programId.run(run9))
      .setStartTime(startTimeSecs)
      .setRunTime(startTimeSecs + 1)
      .setStopTime(startTimeSecs + 5)
      .setStatus(ProgramRunStatus.KILLED)
      .setProperties(noRuntimeArgsProps)
      .setCluster(emptyCluster)
      .setArtifactId(artifactId)
      .setSourceId(AppFabricTestHelper.createSourceId(sourceId)).build();
    RunRecordDetail actualRecord9 = store.getRun(programId.run(run9.getId()));
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

  @Test
  public void testAddApplication() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new FooApp());
    ApplicationId appId = new ApplicationId("account1", "application1");
    ApplicationMeta appMeta = new ApplicationMeta("application1", spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    spec = store.getApplication(appId);
    Assert.assertNotNull(spec);
    Assert.assertEquals(FooMapReduceJob.class.getName(), spec.getMapReduce().get("mrJob1").getClassName());
  }

  @Test
  public void testUpdateChangedApplication() throws ConflictException {
    ApplicationId id = new ApplicationId("account1", "application1");
    ApplicationMeta appMeta = new ApplicationMeta("application1", Specifications.from(new FooApp()),
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(id, appMeta);
    // update
    ApplicationMeta appMetaUpdate = new ApplicationMeta("application1", Specifications.from(new ChangedFooApp()),
                                                        new ChangeDetail(null, null, null,
                                                                         System.currentTimeMillis()));
    store.addLatestApplication(id, appMetaUpdate);

    ApplicationSpecification spec = store.getApplication(id);
    Assert.assertNotNull(spec);
    Assert.assertEquals(FooMapReduceJob.class.getName(), spec.getMapReduce().get("mrJob3").getClassName());
  }

  @Test
  public void testAddApplicationWithoutMarkingLatest()
      throws ConflictException {
    long creationTime = System.currentTimeMillis();
    String appName = "notLatestApp1";
    ApplicationId appId = new ApplicationId("account1", appName, "v1");
    ApplicationMeta appMeta = new ApplicationMeta(appName, Specifications.from(new FooApp()),
        new ChangeDetail(null, null, null, creationTime), null);
    store.addApplication(appId, appMeta, false);

    ApplicationMeta storedMeta = store.getApplicationMetadata(appId);
    Assert.assertEquals(appName, storedMeta.getId());
    Assert.assertEquals(creationTime, storedMeta.getChange().getCreationTimeMillis());

    ApplicationMeta latestMeta = store.getLatest(appId.getAppReference());
    Assert.assertNull(latestMeta);
  }

  @Test
  public void testMarkApplicationsLatestWithNewApps()
      throws ApplicationNotFoundException, ConflictException, IOException {
    long creationTime = System.currentTimeMillis();
    // Add 2 new applications without marking them latest
    ApplicationId appId1 = new ApplicationId("account1", "newApp1");
    ApplicationMeta appMeta1 = new ApplicationMeta("newApp1", Specifications.from(new FooApp()),
        new ChangeDetail(null, null, null, creationTime), null);
    store.addApplication(appId1, appMeta1, false);

    ApplicationId appId2 = new ApplicationId("account1", "newApp2");
    ApplicationMeta appMeta2 = new ApplicationMeta("newApp2", Specifications.from(new FooApp()),
        new ChangeDetail(null, null, null, creationTime), null);
    store.addApplication(appId2, appMeta2, false);

    // Now mark them as latest in bulk
    store.markApplicationsLatest(Arrays.asList(appId1, appId2));

    ApplicationMeta storedMeta1 = store.getLatest(appId1.getAppReference());
    Assert.assertEquals("newApp1", storedMeta1.getId());
    Assert.assertEquals(creationTime, storedMeta1.getChange().getCreationTimeMillis());

    ApplicationMeta storedMeta2 = store.getLatest(appId2.getAppReference());
    Assert.assertEquals("newApp2", storedMeta2.getId());
    Assert.assertEquals(creationTime, storedMeta2.getChange().getCreationTimeMillis());
  }

  @Test
  public void testMarkApplicationsLatestWithExistingLatest()
      throws ApplicationNotFoundException, ConflictException, IOException {
    long creationTime = System.currentTimeMillis();
    long v2CreationTime = creationTime + 1000;
    String appName = "testAppWithVersion";
    String oldVersion = "old-version";
    String newVersion = "new-version";

    // Add an application as latest
    ApplicationId appIdV1 = new ApplicationId("account1", appName, oldVersion);
    ApplicationMeta appMetaV1 = new ApplicationMeta(appName, Specifications.from(new FooApp(), appName, oldVersion),
        new ChangeDetail(null, null, null, creationTime));
    store.addLatestApplication(appIdV1, appMetaV1);

    // Add a new version of the application without marking latest
    ApplicationId appIdV2 = new ApplicationId("account1", appName, newVersion);
    ApplicationMeta appMetaV2 = new ApplicationMeta(appName, Specifications.from(new FooApp(), appName, newVersion),
        new ChangeDetail(null, null, null, v2CreationTime), null);
    store.addApplication(appIdV2, appMetaV2, false);

    // Now mark the new version as latest
    store.markApplicationsLatest(Collections.singletonList(appIdV2));
    ApplicationMeta latestMeta = store.getLatest(appIdV1.getAppReference());

    ApplicationMeta storedMetaV1 = store.getApplicationMetadata(appIdV1);
    Assert.assertEquals(appName, storedMetaV1.getId());
    Assert.assertEquals(oldVersion, storedMetaV1.getSpec().getAppVersion());
    Assert.assertEquals(creationTime, storedMetaV1.getChange().getCreationTimeMillis());
    Assert.assertNotEquals(latestMeta.getSpec().getAppVersion(), storedMetaV1.getSpec().getAppVersion());

    ApplicationMeta storedMetaV2 = store.getApplicationMetadata(appIdV2);
    Assert.assertEquals(appName, storedMetaV2.getId());
    Assert.assertEquals(newVersion, storedMetaV2.getSpec().getAppVersion());
    Assert.assertEquals(v2CreationTime, storedMetaV2.getChange().getCreationTimeMillis());
    Assert.assertEquals(latestMeta.getSpec().getAppVersion(), storedMetaV2.getSpec().getAppVersion());
  }

  @Test(expected = ApplicationNotFoundException.class)
  public void testMarkApplicationsLatestWithNonExistingApp()
      throws ApplicationNotFoundException, IOException {
    // Add an application as latest
    ApplicationId appId = new ApplicationId("account1", "app");

    // Now try marking this non existing app as latest
    // this should throw ApplicationNotFoundException (expected in this test)
    store.markApplicationsLatest(Collections.singletonList(appId));
  }

  @Test
  public void testUpdateApplicationScmMeta()
      throws IOException, ConflictException {
    // Add an application with a scm meta field
    ApplicationId appId = new ApplicationId("account1", "application1");
    ApplicationMeta appMeta = new ApplicationMeta("application1", Specifications.from(new FooApp()),
        new ChangeDetail(null, null, null,
            System.currentTimeMillis()), null);

    store.addLatestApplication(appId, appMeta);
    Map<ApplicationId, SourceControlMeta> updateRequests = new HashMap<>();
    updateRequests.put(appId, new SourceControlMeta("updated-file-hash", "commitId", Instant.now()));
    store.updateApplicationSourceControlMeta(updateRequests);

    ApplicationMeta storedMeta = store.getApplicationMetadata(appId);
    Assert.assertNotNull(storedMeta);
    Assert.assertNotNull(storedMeta.getSourceControlMeta());
    Assert.assertEquals("updated-file-hash", storedMeta.getSourceControlMeta().getFileHash());
  }

  @Test
  public void testUpdateApplicationScmMetaWithNonExistingAppIds()
      throws IOException, ConflictException {
    // Add an application with a scm meta field
    ApplicationId appId = new ApplicationId("account1", "application1");
    ApplicationMeta appMeta = new ApplicationMeta("application1", Specifications.from(new FooApp()),
        new ChangeDetail(null, null, null,
            System.currentTimeMillis()), new SourceControlMeta("initial-file-hash", "commitId",
        Instant.now()));

    store.addLatestApplication(appId, appMeta);
    // The following appId is not added to the store
    ApplicationId appId2 = new ApplicationId("account1", "application2");

    Map<ApplicationId, SourceControlMeta> updateRequests = new HashMap<>();
    updateRequests.put(appId, new SourceControlMeta("updated-file-hash", "commitId", Instant.now()));
    updateRequests.put(appId2, new SourceControlMeta("updated-file-hash-2", "commitId", Instant.now()));
    store.updateApplicationSourceControlMeta(updateRequests);

    ApplicationMeta storedMeta = store.getApplicationMetadata(appId);
    Assert.assertNotNull(storedMeta);
    Assert.assertNotNull(storedMeta.getSourceControlMeta());
    Assert.assertEquals("updated-file-hash", storedMeta.getSourceControlMeta().getFileHash());
  }

  private static class FooApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("FooApp");
      setDescription("Foo App");
      createDataset("dataset1", Table.class);
      createDataset("dataset2", KeyValueTable.class);
      addMapReduce(new FooMapReduceJob("mrJob1"));
      addMapReduce(new FooMapReduceJob("mrJob2"));
    }
  }

  private static class ChangedFooApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("FooApp");
      setDescription("Foo App");
      createDataset("dataset2", KeyValueTable.class);

      createDataset("dataset3", IndexedTable.class,
                    DatasetProperties.builder().add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "foo").build());
      addMapReduce(new FooMapReduceJob("mrJob2"));
      addMapReduce(new FooMapReduceJob("mrJob3"));
    }
  }

  /**
   * Map reduce job for testing MDS.
   */
  public static class FooMapReduceJob extends AbstractMapReduce {
    private final String name;

    FooMapReduceJob(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing MDS");
    }
  }

  @Test
  public void testServiceDeletion() throws ConflictException {
    // Store the application specification
    AbstractApplication app = new AppWithServices();

    ApplicationSpecification appSpec = Specifications.from(app);
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(appSpec.getName(), appSpec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    AbstractApplication newApp = new AppWithNoServices();

    // get the delete program specs after deploying AppWithNoServices
    List<ProgramSpecification> programSpecs = store.getDeletedProgramSpecifications(appId.getAppReference(),
                                                                                    Specifications.from(newApp));

    //verify the result.
    Assert.assertEquals(1, programSpecs.size());
    Assert.assertEquals("NoOpService", programSpecs.get(0).getName());
  }

  @Test
  public void testServiceInstances() throws ConflictException {
    ApplicationSpecification appSpec = Specifications.from(new AppWithServices());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(appSpec.getName(), appSpec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

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
  public void testWorkerInstances() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new AppWithWorker());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

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
  public void testRemoveAll() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app("application1");
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    Assert.assertNotNull(store.getApplication(appId));

    // removing everything
    store.removeAll(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRemoveApplication() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app(spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    Assert.assertNotNull(store.getApplication(appId));

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testProgramRunCount() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    ArtifactId testArtifact = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ProgramId workflowId = appId.workflow(AllProgramsApp.NoOpWorkflow.NAME);
    ProgramId serviceId = appId.service(AllProgramsApp.NoOpService.NAME);
    ProgramId nonExistingAppProgramId = NamespaceId.DEFAULT.app("nonExisting").workflow("test");
    ProgramId nonExistingProgramId = appId.workflow("nonExisting");

    // add the application
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    // add some run records to workflow and service
    for (int i = 0; i < 5; i++) {
      setStart(workflowId.run(RunIds.generate()), Collections.emptyMap(), Collections.emptyMap(), testArtifact);
      setStart(serviceId.run(RunIds.generate()), Collections.emptyMap(), Collections.emptyMap(), testArtifact);
    }

    List<RunCountResult> result =
      store.getProgramTotalRunCounts(ImmutableList.of(workflowId.getProgramReference(),
                                                 serviceId.getProgramReference(),
                                                 nonExistingAppProgramId.getProgramReference(),
                                                 nonExistingProgramId.getProgramReference()));

    // compare the result
    Assert.assertEquals(4, result.size());
    for (RunCountResult runCountResult : result) {
      ProgramReference programReference = runCountResult.getProgramReference();
      Long count = runCountResult.getCount();
      if (programReference.equals(nonExistingAppProgramId.getProgramReference())
          || programReference.equals(nonExistingProgramId.getProgramReference())) {
        Assert.assertNull(count);
        Assert.assertTrue(runCountResult.getException() instanceof NotFoundException);
      } else {
        Assert.assertNotNull(count);
        Assert.assertEquals(5L, count.longValue());
      }
    }

    // remove the app should remove all run count
    store.removeApplication(appId);
    for (RunCountResult runCountResult :
      store.getProgramTotalRunCounts(ImmutableList.of(workflowId.getProgramReference(),
                                                      serviceId.getProgramReference()))) {
      Assert.assertNull(runCountResult.getCount());
      Assert.assertTrue(runCountResult.getException() instanceof NotFoundException);
    }
  }

  @Test
  public void testRuntimeArgsDeletion() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = new ApplicationId("testDeleteRuntimeArgs", spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    Assert.assertNotNull(store.getApplication(appId));

    ProgramId mapreduceProgramId = appId.mr("NoOpMR");
    ProgramId workflowProgramId = appId.workflow("NoOpWorkflow");

    long nowMillis = System.currentTimeMillis();
    String mapreduceRunId = RunIds.generate(nowMillis).getId();
    String workflowRunId = RunIds.generate(nowMillis).getId();

    ArtifactId artifactId = appId.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();

    setStartAndRunning(mapreduceProgramId.run(mapreduceRunId),
                       ImmutableMap.of("path", "/data"), new HashMap<>(), artifactId);
    setStartAndRunning(workflowProgramId.run(workflowRunId),
                       ImmutableMap.of("whitelist", "cask"), new HashMap<>(), artifactId);

    ProgramRunId mapreduceProgramRunId = mapreduceProgramId.run(mapreduceRunId);
    ProgramRunId workflowProgramRunId = workflowProgramId.run(workflowRunId);

    Map<String, String> args = store.getRuntimeArguments(mapreduceProgramRunId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("/data", args.get("path"));

    args = store.getRuntimeArguments(workflowProgramRunId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("cask", args.get("whitelist"));

    // removing application
    store.removeApplication(appId);

    //Check if args are deleted.
    args = store.getRuntimeArguments(mapreduceProgramRunId);
    Assert.assertEquals(0, args.size());

    args = store.getRuntimeArguments(workflowProgramRunId);
    Assert.assertEquals(0, args.size());
  }

  @Test
  public void testHistoryDeletion() throws ConflictException {

    // Deploy two apps, write some history for programs
    // Remove application using accountId, AppId and verify
    // Remove all from accountId and verify
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("testDeleteAll");
    ApplicationId appId1 = namespaceId.app(spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId1, appMeta);

    spec = Specifications.from(new AppWithServices());
    ApplicationId appId2 = namespaceId.app(spec.getName());
    appMeta = new ApplicationMeta(spec.getName(), spec,
                                  new ChangeDetail(null, null, null,
                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId2, appMeta);

    ArtifactId artifactId = appId1.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();

    Assert.assertNotNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    long now = System.currentTimeMillis();
    ProgramId mapreduceProgramId1 = appId1.mr("NoOpMR");
    ProgramId workflowProgramId1 = appId1.workflow("NoOpWorkflow");

    ProgramRunId mapreduceProgramRunId1 = mapreduceProgramId1.run(RunIds.generate(now - 1000));
    setStartAndRunning(mapreduceProgramRunId1, artifactId);
    store.setStop(mapreduceProgramRunId1, now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    RunId runId = RunIds.generate(now - 1000);
    setStartAndRunning(workflowProgramId1.run(runId.getId()), artifactId);
    store.setStop(workflowProgramId1.run(runId.getId()), now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    ProgramId serviceId = appId2.service(AppWithServices.SERVICE_NAME);
    ProgramRunId serviceRunId = serviceId.run(RunIds.generate(now - 1000));
    setStartAndRunning(serviceRunId, artifactId);
    store.setStop(serviceRunId, now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    verifyRunHistory(mapreduceProgramId1, 1);
    verifyRunHistory(workflowProgramId1, 1);

    verifyRunHistory(serviceId, 1);

    // removing application
    store.removeApplication(appId1);

    Assert.assertNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    verifyRunHistory(mapreduceProgramId1, 0);
    verifyRunHistory(workflowProgramId1, 0);

    // Check to see if the history of second app is not deleted
    verifyRunHistory(serviceId, 1);

    // remove all
    store.removeAll(namespaceId);

    verifyRunHistory(serviceId, 0);
  }

  private void verifyRunHistory(ProgramId programId, int count) {
    Map<ProgramRunId, RunRecordDetail> historymap = store.getRuns(programId, ProgramRunStatus.ALL,
                                                                  0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(count, historymap.size());
  }

  @Test
  public void testRunsLimit() throws ConflictException {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = new ApplicationId("testRunsLimit", spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    ProgramId mapreduceProgramId = new ApplicationId("testRunsLimit", spec.getName())
      .mr(AllProgramsApp.NoOpMR.class.getSimpleName());
    ArtifactId artifactId = appId.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();

    Assert.assertNotNull(store.getApplication(appId));

    long now = System.currentTimeMillis();
    ProgramRunId programRunId = mapreduceProgramId.run(RunIds.generate(now - 3000));
    setStartAndRunning(programRunId, artifactId);
    store.setStop(programRunId, now - 100, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    setStartAndRunning(mapreduceProgramId.run(RunIds.generate(now - 2000)), artifactId);

    // even though there's two separate run records (one that's complete and one that's active), only one should be
    // returned by the query, because the limit parameter of 1 is being passed in.
    Map<ProgramRunId, RunRecordDetail> historymap = store.getRuns(mapreduceProgramId, ProgramRunStatus.ALL,
                                                                  0, Long.MAX_VALUE, 1);
    Assert.assertEquals(1, historymap.size());
  }

  @Test
  public void testCheckDeletedProgramSpecs() throws ConflictException {
    //Deploy program with all types of programs.
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    Set<String> specsToBeVerified = Sets.newHashSet();
    specsToBeVerified.addAll(spec.getMapReduce().keySet());
    specsToBeVerified.addAll(spec.getWorkflows().keySet());
    specsToBeVerified.addAll(spec.getServices().keySet());
    specsToBeVerified.addAll(spec.getWorkers().keySet());
    specsToBeVerified.addAll(spec.getSpark().keySet());

    //Verify if there are 6 program specs in AllProgramsApp
    Assert.assertEquals(6, specsToBeVerified.size());

    // Check the diff with the same app - re-deployment scenario where programs are not removed.
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId.getAppReference(), spec);
    Assert.assertEquals(0, deletedSpecs.size());

    //Get the spec for app that contains no programs.
    spec = Specifications.from(new NoProgramsApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    deletedSpecs = store.getDeletedProgramSpecifications(appId.getAppReference(), spec);
    Assert.assertEquals(6, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeVerified.remove(specification.getName());
    }

    //All the 6 specs should have been deleted.
    Assert.assertEquals(0, specsToBeVerified.size());
  }

  @Test
  public void testScanApplications() throws ConflictException {
    testScanApplications(store);
  }

  protected void testScanApplications(Store store) throws ConflictException {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    int count = 100;
    for (int i = 0; i < count; i++) {
      String appName = "test" + i;
      ApplicationMeta appMeta = new ApplicationMeta(appName, appSpec,
                                                    new ChangeDetail(null, null, null,
                                                                     System.currentTimeMillis()));
      store.addLatestApplication(new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appName), appMeta);
    }

    // Mimicking editing the first count / 2 apps
    for (int i = 0; i < count / 2; i++) {
      String appName = "test" + i;
      String version = "version" + i;
      ApplicationMeta appMeta = new ApplicationMeta(appName, appSpec,
          new ChangeDetail("edited" + i, null, null,
              System.currentTimeMillis()));
      store.addLatestApplication(new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appName, version), appMeta);
    }

    List<ApplicationId> allAppsVersion = new ArrayList<>();
    store.scanApplications(ScanApplicationsRequest.builder().build(),
        20, (appId, spec) -> allAppsVersion.add(appId));

    Assert.assertEquals(count + count / 2, allAppsVersion.size());

    List<ApplicationId> latestApps = new ArrayList<>();
    store.scanApplications(20, (appId, spec) -> latestApps.add(appId));

    Assert.assertEquals(count, latestApps.size());

    //Reverse
    List<ApplicationId> reverseApps = new ArrayList<>();
    Assert.assertFalse(store.scanApplications(ScanApplicationsRequest.builder()
        .setSortOrder(SortOrder.DESC)
        .setLatestOnly(true)
        .build(), 20, (appId, spec) -> reverseApps.add(appId)));
    Assert.assertEquals(Lists.reverse(latestApps), reverseApps);

    //Second page
    int firstPageSize = 10;
    List<ApplicationId> restartApps = new ArrayList<>();
    Assert.assertFalse(store.scanApplications(ScanApplicationsRequest.builder()
        .setScanFrom(latestApps.get(firstPageSize - 1)).setLatestOnly(true)
        .build(), 20, (appId, spec) -> restartApps.add(appId)));
    Assert.assertEquals(latestApps.subList(firstPageSize, latestApps.size()), restartApps);
  }

  @Test
  public void testScanApplicationsWithNamespace() throws ConflictException {
    testScanApplicationsWithNamespace(store);
  }

  public void testScanApplicationsWithNamespace(Store store) throws ConflictException {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    int count = 100;
    for (int i = 0; i < count / 2; i++) {
      String appName = "test" + (2 * i);
      ApplicationMeta appMeta = new ApplicationMeta(appName, appSpec,
                                                    new ChangeDetail(null, null, null,
                                                                     System.currentTimeMillis()));
      store.addLatestApplication(new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appName), appMeta);
      appName = "test" + (2 * i + 1);
      store.addLatestApplication(new ApplicationId(NamespaceId.CDAP.getNamespace(), appName), appMeta);
    }

    List<ApplicationId> apps = new ArrayList<ApplicationId>();

    ScanApplicationsRequest request = ScanApplicationsRequest.builder()
      .setNamespaceId(NamespaceId.CDAP).build();

    Assert.assertFalse(store.scanApplications(request, 20, (appId, spec) -> {
      apps.add(appId);
    }));

    Assert.assertEquals(count / 2, apps.size());

    //Reverse
    List<ApplicationId> reverseApps = new ArrayList<>();
    request = ScanApplicationsRequest.builder()
      .setNamespaceId(NamespaceId.CDAP)
      .setSortOrder(SortOrder.DESC)
      .build();
    Assert.assertFalse(store.scanApplications(request, 20, (appId, spec) -> reverseApps.add(appId)));
    Assert.assertEquals(Lists.reverse(apps), reverseApps);

    int firstPageSize = 10;
    //First page - DESC
    {
      List<ApplicationId> firstPageApps = new ArrayList<>();
      request = ScanApplicationsRequest.builder()
        .setNamespaceId(NamespaceId.CDAP)
        .setSortOrder(SortOrder.DESC)
        .setLimit(firstPageSize)
        .build();
      Assert.assertTrue(store.scanApplications(request, 20, (appId, spec) -> firstPageApps.add(appId)));
      Assert.assertEquals(Lists.reverse(apps).subList(0, firstPageSize), firstPageApps);
    }

    //First page - ASC
    {
      List<ApplicationId> firstPageApps = new ArrayList<>();
      request = ScanApplicationsRequest.builder()
        .setNamespaceId(NamespaceId.CDAP)
        .setLimit(firstPageSize)
        .build();
      Assert.assertTrue(store.scanApplications(request, 20, (appId, spec) -> firstPageApps.add(appId)));
      Assert.assertEquals(apps.subList(0, firstPageSize), firstPageApps);
    }

    //Remaining items
    List<ApplicationId> restartApps = new ArrayList<>();
    request = ScanApplicationsRequest.builder()
      .setNamespaceId(NamespaceId.CDAP)
      .setScanFrom(apps.get(firstPageSize - 1))
      .build();
    Assert.assertFalse(store.scanApplications(request, 20, (appId, spec) -> restartApps.add(appId)));
    Assert.assertEquals(apps.subList(firstPageSize, apps.size()), restartApps);
  }

  @Test
  public void testCheckDeletedWorkflow() throws ConflictException {
    //Deploy program with all types of programs.
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);

    Set<String> specsToBeDeleted = Sets.newHashSet();
    specsToBeDeleted.addAll(spec.getWorkflows().keySet());

    Assert.assertEquals(1, specsToBeDeleted.size());

    //Get the spec for app that contains only flow and mapreduce - removing workflows.
    spec = Specifications.from(new DefaultStoreTestApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId.getAppReference(), spec);
    Assert.assertEquals(2, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeDeleted.remove(specification.getName());
    }

    //2 specs should have been deleted and 0 should be remaining.
    Assert.assertEquals(0, specsToBeDeleted.size());
  }

  @Test
  public void testRunningInRangeSimple() {
    NamespaceId ns = new NamespaceId("d");
    final ProgramRunId run1 = ns.app("a1").program(ProgramType.SERVICE, "f1").run(RunIds.generate(20000).getId());
    final ProgramRunId run2 = ns.app("a2").program(ProgramType.MAPREDUCE, "f2").run(RunIds.generate(10000).getId());
    final ProgramRunId run3 = ns.app("a3").program(ProgramType.WORKER, "f3").run(RunIds.generate(40000).getId());
    final ProgramRunId run4 = ns.app("a4").program(ProgramType.SERVICE, "f4").run(RunIds.generate(70000).getId());
    final ProgramRunId run5 = ns.app("a5").program(ProgramType.SPARK, "f5").run(RunIds.generate(30000).getId());
    final ProgramRunId run6 = ns.app("a6").program(ProgramType.WORKFLOW, "f6").run(RunIds.generate(60000).getId());
    ArtifactId artifactId = ns.artifact("testArtifact", "1.0").toApiArtifactId();
    writeStartRecord(run1, artifactId);
    writeStartRecord(run2, artifactId);
    writeStartRecord(run3, artifactId);
    writeStartRecord(run4, artifactId);
    writeStartRecord(run5, artifactId);
    writeStartRecord(run6, artifactId);

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
  public void testRunningInRangeMulti() {
    // Add some run records
    TreeSet<Long> allPrograms = new TreeSet<>();
    TreeSet<Long> stoppedPrograms = new TreeSet<>();
    TreeSet<Long> suspendedPrograms = new TreeSet<>();
    TreeSet<Long> runningPrograms = new TreeSet<>();
    for (int i = 0; i < 99; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app" + i);
      ArtifactId artifactId = application.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();
      ProgramId program = application.program(ProgramType.values()[i % ProgramType.values().length],
                                              "program" + i);
      long startTime = (i + 1) * 10000;
      RunId runId = RunIds.generate(startTime);
      allPrograms.add(startTime);
      ProgramRunId run = program.run(runId.getId());
      writeStartRecord(run, artifactId);

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

  @Test
  public void testStateRemovedOnRemoveApplication() throws ApplicationNotFoundException, ConflictException {
    String stateKey = "kafka";
    byte[] stateValue = ("{\n"
        + "\"offset\" : 12345\n"
        + "}").getBytes(StandardCharsets.UTF_8);

    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app(spec.getName());
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);
    store.saveState(new AppStateKeyValue(namespaceId, spec.getName(), stateKey, stateValue));

    Assert.assertNotNull(store.getApplication(appId));
    AppStateKey appStateRequest = new AppStateKey(namespaceId, spec.getName(), stateKey);
    Assert.assertNotNull(store.getState(appStateRequest));

    // removing application should work successfully
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testStateRemovedOnRemoveAll() throws ApplicationNotFoundException, ConflictException {
    String stateKey = "kafka";
    byte[] stateValue = ("{\n"
        + "\"offset\" : 12345\n"
        + "}").getBytes(StandardCharsets.UTF_8);
    String appName = "application1";

    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app(appName);
    ApplicationMeta appMeta = new ApplicationMeta(spec.getName(), spec,
                                                  new ChangeDetail(null, null, null,
                                                                   System.currentTimeMillis()));
    store.addLatestApplication(appId, appMeta);
    store.saveState(new AppStateKeyValue(namespaceId, appName, stateKey, stateValue));

    Assert.assertNotNull(store.getApplication(appId));
    AppStateKey appStateRequest = new AppStateKey(namespaceId, appName, stateKey);
    Assert.assertNotNull(store.getState(appStateRequest));

    // removing everything should work successfully
    store.removeAll(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  private ApplicationSpecification createDummyAppSpec(String appName, String appVersion, ArtifactId artifactId) {
    return new DefaultApplicationSpecification(
      appName, appVersion, ProjectInfo.getVersion().toString(), "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
  }

  @Test
  public void testListRunsWithLegacyRows() throws ConflictException {
    String appName = "application1";
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ApplicationSpecification spec = createDummyAppSpec(appId.getApplication(), appId.getVersion(), artifactId);
    ApplicationMeta appMeta = new ApplicationMeta(appId.getApplication(), spec, null);
    List<ApplicationId> expectedApps = new ArrayList<>();

    // Insert a row that is null for changeDetail
    store.addLatestApplication(appId, appMeta);
    expectedApps.add(appId);

    ApplicationId newVersionAppId = appId.getAppReference().app("new_version");
    spec = createDummyAppSpec(newVersionAppId.getApplication(), newVersionAppId.getVersion(), artifactId);
    long currentTime = System.currentTimeMillis();

    ApplicationMeta newAppMeta = new ApplicationMeta(newVersionAppId.getApplication(), spec,
                                                     new ChangeDetail(null, null,
                                                                      null, currentTime));
    // Insert a second version
    store.addLatestApplication(newVersionAppId, newAppMeta);
    expectedApps.add(newVersionAppId);

    // Insert a third version
    ApplicationId anotherVersionAppId = appId.getAppReference().app("another_version");
    spec = createDummyAppSpec(anotherVersionAppId.getApplication(), anotherVersionAppId.getVersion(), artifactId);
    ApplicationMeta anotherAppMeta = new ApplicationMeta(anotherVersionAppId.getApplication(), spec,
                                                         new ChangeDetail(null, null,
                                                                          null, currentTime + 1000));
    store.addLatestApplication(anotherVersionAppId, anotherAppMeta);
    expectedApps.add(anotherVersionAppId);

    // Reverse it because we want DESC order
    Collections.reverse(expectedApps);

    List<ApplicationId> actualApps = new ArrayList<>();
    List<Long> creationTimes = new ArrayList<>();
    AtomicInteger latestVersionCount = new AtomicInteger();
    AtomicReference<ApplicationId> latestAppId = new AtomicReference<>();

    ScanApplicationsRequest request = ScanApplicationsRequest.builder()
      .setNamespaceId(NamespaceId.DEFAULT)
      .setSortOrder(SortOrder.DESC)
      .setSortCreationTime(true)
      .setLimit(10)
      .build();

    Assert.assertFalse(store.scanApplications(request, 20, (id, appSpec) -> {
      actualApps.add(id);
      creationTimes.add(appSpec.getChange().getCreationTimeMillis());
      if (Boolean.TRUE.equals(appSpec.getChange().getLatest())) {
        latestVersionCount.getAndIncrement();
        latestAppId.set(id);
      }
    }));

    Assert.assertEquals(expectedApps, actualApps);
    Assert.assertEquals(creationTimes.size(), 3);
    Assert.assertEquals(creationTimes.get(1) - 1000, (long) creationTimes.get(2));
    Assert.assertEquals(latestVersionCount.get(), 1);
    Assert.assertEquals(latestAppId.get(), actualApps.get(0));
  }

  private void writeStartRecord(ProgramRunId run, ArtifactId artifactId) {
    setStartAndRunning(run, artifactId);
    Assert.assertNotNull(store.getRun(run));
  }

  private void writeStopRecord(ProgramRunId run, long stopTimeInMillis) {
    store.setStop(run, TimeUnit.MILLISECONDS.toSeconds(stopTimeInMillis),
                  ProgramRunStatus.COMPLETED, AppFabricTestHelper.createSourceId(++sourceId));
    Assert.assertNotNull(store.getRun(run));
  }

  private void writeSuspendedRecord(ProgramRunId run) {
    store.setSuspend(run, AppFabricTestHelper.createSourceId(++sourceId), -1);
    Assert.assertNotNull(store.getRun(run));
  }

  private Set<Long> runsToTime(ProgramRunId... runIds) {
    return Arrays.stream(runIds)
      .map(ProgramRunId::getRun)
      .map(RunIds::fromString)
      .map(id -> RunIds.getTime(id, TimeUnit.MILLISECONDS))
      .collect(Collectors.toCollection(TreeSet::new));
  }

  private SortedSet<Long> runIdsToTime(Set<RunId> runIds) {
    return runIds.stream()
      .map(runId -> RunIds.getTime(runId, TimeUnit.MILLISECONDS))
      .collect(Collectors.toCollection(TreeSet::new));
  }
}
