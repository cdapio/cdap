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
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.store.DefaultNamespaceStore;
import org.apache.twill.api.RunId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
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
import java.util.stream.Collectors;

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
    store.addApplication(appId, appSpec);

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
    String sparkName = "spark1";

    ApplicationId appId = Ids.namespace(namespaceName).app(appName);
    ProgramId mapReduceProgram = appId.mr(mapReduceName);
    ProgramId sparkProgram = appId.spark(sparkName);

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

    setStartAndRunning(mapReduceProgram.run(mapReduceRunId.getId()), ImmutableMap.of(), systemArgs, artifactId);

    // stop the MapReduce program
    store.setStop(mapReduceProgram.run(mapReduceRunId.getId()), currentTime + 50, ProgramRunStatus.COMPLETED,
                  AppFabricTestHelper.createSourceId(++sourceId));

    // start Spark program as a part of Workflow
    systemArgs = ImmutableMap.of(ProgramOptionConstants.WORKFLOW_NODE_ID, sparkName,
                                 ProgramOptionConstants.WORKFLOW_NAME, workflowName,
                                 ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId);

    RunId sparkRunId = RunIds.generate(currentTime + 60);
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
    Map<String, String> noRuntimeArgsProps = ImmutableMap.of("runtimeArgs",
                                                             GSON.toJson(ImmutableMap.<String, String>of()));
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
  public void testAddApplication() {
    ApplicationSpecification spec = Specifications.from(new FooApp());
    ApplicationId appId = new ApplicationId("account1", "application1");
    store.addApplication(appId, spec);

    spec = store.getApplication(appId);
    Assert.assertNotNull(spec);
    Assert.assertEquals(FooMapReduceJob.class.getName(), spec.getMapReduce().get("mrJob1").getClassName());
  }

  @Test
  public void testUpdateChangedApplication() {
    ApplicationId id = new ApplicationId("account1", "application1");

    store.addApplication(id, Specifications.from(new FooApp()));
    // update
    store.addApplication(id, Specifications.from(new ChangedFooApp()));

    ApplicationSpecification spec = store.getApplication(id);
    Assert.assertNotNull(spec);
    Assert.assertEquals(FooMapReduceJob.class.getName(), spec.getMapReduce().get("mrJob3").getClassName());
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
  public void testServiceDeletion() {
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
  public void testServiceInstances() {
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
  public void testWorkerInstances() {
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
  public void testRemoveAll() {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app("application1");
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    // removing everything
    store.removeAll(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRemoveApplication() {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("account1");
    ApplicationId appId = namespaceId.app(spec.getName());
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testProgramRunCount() {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    ArtifactId testArtifact = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ProgramId workflowId = appId.workflow(AllProgramsApp.NoOpWorkflow.NAME);
    ProgramId serviceId = appId.service(AllProgramsApp.NoOpService.NAME);
    ProgramId nonExistingAppProgramId = NamespaceId.DEFAULT.app("nonExisting").workflow("test");
    ProgramId nonExistingProgramId = appId.workflow("nonExisting");

    // add the application
    store.addApplication(appId, spec);

    // add some run records to workflow and service
    for (int i = 0; i < 5; i++) {
      setStart(workflowId.run(RunIds.generate()), Collections.emptyMap(), Collections.emptyMap(), testArtifact);
      setStart(serviceId.run(RunIds.generate()), Collections.emptyMap(), Collections.emptyMap(), testArtifact);
    }

    List<RunCountResult> result = store.getProgramRunCounts(ImmutableList.of(workflowId, serviceId,
                                                                             nonExistingAppProgramId,
                                                                             nonExistingProgramId));

    // compare the result
    Assert.assertEquals(4, result.size());
    for (RunCountResult runCountResult : result) {
      ProgramId programId = runCountResult.getProgramId();
      Long count = runCountResult.getCount();
      if (programId.equals(nonExistingAppProgramId) || programId.equals(nonExistingProgramId)) {
        Assert.assertNull(count);
        Assert.assertTrue(runCountResult.getException() instanceof NotFoundException);
      } else {
        Assert.assertNotNull(count);
        Assert.assertEquals(5L, count.longValue());
      }
    }

    // remove the app should remove all run count
    store.removeApplication(appId);
    for (RunCountResult runCountResult : store.getProgramRunCounts(ImmutableList.of(workflowId, serviceId))) {
      Assert.assertNull(runCountResult.getCount());
      Assert.assertTrue(runCountResult.getException() instanceof NotFoundException);
    }
  }

  @Test
  public void testRuntimeArgsDeletion() {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = new ApplicationId("testDeleteRuntimeArgs", spec.getName());
    store.addApplication(appId, spec);

    Assert.assertNotNull(store.getApplication(appId));

    ProgramId mapreduceProgramId = appId.mr("NoOpMR");
    ProgramId workflowProgramId = appId.workflow("NoOpWorkflow");

    long nowMillis = System.currentTimeMillis();
    String mapreduceRunId = RunIds.generate(nowMillis).getId();
    String workflowRunId = RunIds.generate(nowMillis).getId();

    ProgramRunId mapreduceProgramRunId = mapreduceProgramId.run(mapreduceRunId);
    ProgramRunId workflowProgramRunId = workflowProgramId.run(workflowRunId);
    ArtifactId artifactId = appId.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();

    setStartAndRunning(mapreduceProgramId.run(mapreduceRunId),
                       ImmutableMap.of("path", "/data"), new HashMap<>(), artifactId);
    setStartAndRunning(workflowProgramId.run(workflowRunId),
                       ImmutableMap.of("whitelist", "cask"), new HashMap<>(), artifactId);

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
  public void testHistoryDeletion() {

    // Deploy two apps, write some history for programs
    // Remove application using accountId, AppId and verify
    // Remove all from accountId and verify
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    NamespaceId namespaceId = new NamespaceId("testDeleteAll");
    ApplicationId appId1 = namespaceId.app(spec.getName());
    store.addApplication(appId1, spec);

    spec = Specifications.from(new AppWithServices());
    ApplicationId appId2 = namespaceId.app(spec.getName());
    store.addApplication(appId2, spec);

    ProgramId mapreduceProgramId1 = appId1.mr("NoOpMR");
    ProgramId workflowProgramId1 = appId1.workflow("NoOpWorkflow");
    ArtifactId artifactId = appId1.getNamespaceId().artifact("testArtifact", "1.0").toApiArtifactId();

    ProgramId serviceId = appId2.service(AppWithServices.SERVICE_NAME);

    Assert.assertNotNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    long now = System.currentTimeMillis();

    ProgramRunId mapreduceProgramRunId1 = mapreduceProgramId1.run(RunIds.generate(now - 1000));
    setStartAndRunning(mapreduceProgramRunId1, artifactId);
    store.setStop(mapreduceProgramRunId1, now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

    RunId runId = RunIds.generate(now - 1000);
    setStartAndRunning(workflowProgramId1.run(runId.getId()), artifactId);
    store.setStop(workflowProgramId1.run(runId.getId()), now, ProgramController.State.COMPLETED.getRunStatus(),
                  AppFabricTestHelper.createSourceId(++sourceId));

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
  public void testRunsLimit() {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = new ApplicationId("testRunsLimit", spec.getName());
    store.addApplication(appId, spec);

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
  public void testCheckDeletedProgramSpecs() {
    //Deploy program with all types of programs.
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    store.addApplication(appId, spec);

    Set<String> specsToBeVerified = Sets.newHashSet();
    specsToBeVerified.addAll(spec.getMapReduce().keySet());
    specsToBeVerified.addAll(spec.getWorkflows().keySet());
    specsToBeVerified.addAll(spec.getServices().keySet());
    specsToBeVerified.addAll(spec.getWorkers().keySet());
    specsToBeVerified.addAll(spec.getSpark().keySet());

    //Verify if there are 6 program specs in AllProgramsApp
    Assert.assertEquals(6, specsToBeVerified.size());

    // Check the diff with the same app - re-deployment scenario where programs are not removed.
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(0, deletedSpecs.size());

    //Get the spec for app that contains no programs.
    spec = Specifications.from(new NoProgramsApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(6, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeVerified.remove(specification.getName());
    }

    //All the 6 specs should have been deleted.
    Assert.assertEquals(0, specsToBeVerified.size());
  }

  @Test
  public void testScanApplications() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    int count = 100;
    for (int i = 0; i < count; i++) {
      String appName = "test" + i;
      store.addApplication(new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appName), appSpec);
    }

    List<ApplicationId> apps = new ArrayList<ApplicationId>();
    store.scanApplications(20, (appId, spec) -> apps.add(appId));

    Assert.assertEquals(count, apps.size());
  }

  @Test
  public void testScanApplicationsWithNamespace() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    int count = 100;
    for (int i = 0; i < count / 2; i++) {
      String appName = "test" + (2 * i);
      store.addApplication(new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appName), appSpec);
      appName = "test" + (2 * i + 1);
      store.addApplication(new ApplicationId(NamespaceId.CDAP.getNamespace(), appName), appSpec);
    }

    List<ApplicationId> apps = new ArrayList<ApplicationId>();
    store.scanApplications(NamespaceId.CDAP, 20, (appId, spec) -> apps.add(appId));

    Assert.assertEquals(count / 2, apps.size());
  }

  @Test
  public void testCheckDeletedWorkflow() {
    //Deploy program with all types of programs.
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(spec.getName());
    store.addApplication(appId, spec);

    Set<String> specsToBeDeleted = Sets.newHashSet();
    specsToBeDeleted.addAll(spec.getWorkflows().keySet());

    Assert.assertEquals(1, specsToBeDeleted.size());

    //Get the spec for app that contains only flow and mapreduce - removing workflows.
    spec = Specifications.from(new DefaultStoreTestApp());

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
  public void testRunningInRangeSimple() {
    NamespaceId ns = new NamespaceId("d");
    ProgramRunId run1 = ns.app("a1").program(ProgramType.SERVICE, "f1").run(RunIds.generate(20000).getId());
    ProgramRunId run2 = ns.app("a2").program(ProgramType.MAPREDUCE, "f2").run(RunIds.generate(10000).getId());
    ProgramRunId run3 = ns.app("a3").program(ProgramType.WORKER, "f3").run(RunIds.generate(40000).getId());
    ProgramRunId run4 = ns.app("a4").program(ProgramType.SERVICE, "f4").run(RunIds.generate(70000).getId());
    ProgramRunId run5 = ns.app("a5").program(ProgramType.SPARK, "f5").run(RunIds.generate(30000).getId());
    ProgramRunId run6 = ns.app("a6").program(ProgramType.WORKFLOW, "f6").run(RunIds.generate(60000).getId());
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
