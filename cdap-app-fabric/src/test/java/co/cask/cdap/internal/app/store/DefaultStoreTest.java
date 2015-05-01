/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.FlowMapReduceApp;
import co.cask.cdap.NoProgramsApp;
import co.cask.cdap.ToyApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.IndexedTableDefinition;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.internal.app.Specifications;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.templates.AdapterDefinition;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.internal.DefaultId;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.inject.Injector;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DefaultStoreTest {
  private static final Gson GSON = new Gson();
  private static DefaultStore store;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(DefaultStore.class);
  }

  @Before
  public void before() throws Exception {
    store.clear();
    NamespacedLocationFactory namespacedLocationFactory =
      AppFabricTestHelper.getInjector().getInstance(NamespacedLocationFactory.class);
    namespacedLocationFactory.get(Constants.DEFAULT_NAMESPACE_ID).delete(true);
    NamespaceAdmin admin = AppFabricTestHelper.getInjector().getInstance(NamespaceAdmin.class);
    admin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
  }

  @Test
  public void testLoadingProgram() throws Exception {
    AppFabricTestHelper.deployApplication(ToyApp.class);
    Program program = store.loadProgram(Id.Program.from(DefaultId.NAMESPACE.getId(), "ToyApp",
                                                        ProgramType.FLOW, "ToyFlow"),
                                        ProgramType.FLOW);
    Assert.assertNotNull(program);
  }

  @Test(expected = RuntimeException.class)
  public void testStopBeforeStart() throws RuntimeException {
    Id.Program programId = Id.Program.from("account1", "invalidApp", ProgramType.FLOW, "InvalidFlowOperation");
    long now = System.currentTimeMillis();
    store.setStop(programId, "runx", now, ProgramController.State.ERROR.getRunStatus());
  }

  @Test
  public void testConcurrentStopStart() throws Exception {
    // Two programs that start/stop at same time
    // Should have two run history.
    Id.Program programId = Id.Program.from("account1", "concurrentApp", ProgramType.FLOW, "concurrentFlow");
    long now = System.currentTimeMillis();
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);

    RunId run1 = RunIds.generate(now - 10000);
    store.setStart(programId, run1.getId(), runIdToSecs(run1));
    RunId run2 = RunIds.generate(now - 10000);
    store.setStart(programId, run2.getId(), runIdToSecs(run2));

    store.setStop(programId, run1.getId(), nowSecs, ProgramController.State.COMPLETED.getRunStatus());
    store.setStop(programId, run2.getId(), nowSecs, ProgramController.State.COMPLETED.getRunStatus());

    List<RunRecord> history = store.getRuns(programId, ProgramRunStatus.ALL,
                                            0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(2, history.size());
  }

  @Test
  public void testAdapterLogRunHistory() throws Exception {
    String adapter = "adapter1";
    Id.Program programId = Id.Program.from("ns1", "app1", ProgramType.WORKER, "wrk1");
    long now = System.currentTimeMillis();
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);
    RunId run1 = RunIds.generate(now - 20000);

    // Record start through an Adapter but try to stop the run outside of an adapter.
    store.setStart(programId, run1.getId(), runIdToSecs(run1), adapter, null);

    // RunRecord should be available through RunRecord query for that Program.
    RunRecord programRun = store.getRun(programId, run1.getId());
    Assert.assertEquals(run1.getId(), programRun.getPid());

    store.setStop(programId, run1.getId(), nowSecs - 10, ProgramController.State.COMPLETED.getRunStatus());

    RunRecord adapterRun = store.getRun(programId, run1.getId());
    Assert.assertNotNull(adapterRun);
    Assert.assertEquals(run1.getId(), adapterRun.getPid());

    // RunRecords query for the Program under different Adapter name should not return anything
    List<RunRecord> records = store.getRuns(programId, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                            "invalidAdapter");
    Assert.assertTrue(records.isEmpty());

    // RunRecords query for the Program should return the RunRecord
    List<RunRecord> runRecords = store.getRuns(programId, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(1, runRecords.size());
    Assert.assertEquals(run1.getId(), Iterables.getFirst(runRecords, null).getPid());

    List<RunRecord> adapterRuns = store.getRuns(programId, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE,
                                                adapter);
    List<RunRecord> completedRuns = store.getRuns(programId, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE,
                                                  Integer.MAX_VALUE, adapter);
    Assert.assertEquals(adapterRuns, completedRuns);
    Assert.assertEquals(1, adapterRuns.size());
    Assert.assertEquals(run1.getId(), Iterables.getFirst(adapterRuns, null).getPid());
  }

  @Test
  public void testLogProgramRunHistory() throws Exception {
    // record finished flow
    Id.Program programId = Id.Program.from("account1", "application1", ProgramType.FLOW, "flow1");
    long now = System.currentTimeMillis();
    long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);

    RunId run1 = RunIds.generate(now - 20000);
    store.setStart(programId, run1.getId(), runIdToSecs(run1));
    store.setStop(programId, run1.getId(), nowSecs - 10, ProgramController.State.ERROR.getRunStatus());

    // record another finished flow
    RunId run2 = RunIds.generate(now - 10000);
    store.setStart(programId, run2.getId(), runIdToSecs(run2));
    store.setStop(programId, run2.getId(), nowSecs - 5, ProgramController.State.COMPLETED.getRunStatus());

    // record a suspended flow
    RunId run21 = RunIds.generate(now - 7500);
    store.setStart(programId, run21.getId(), runIdToSecs(run21));
    store.setSuspend(programId, run21.getId());

    // record not finished flow
    RunId run3 = RunIds.generate(now);
    store.setStart(programId, run3.getId(), runIdToSecs(run3));

    // For a RunRecord that has not yet been completed, getStopTs should return null
    RunRecord runRecord = store.getRun(programId, run3.getId());
    Assert.assertNull(runRecord.getStopTs());

    // record run of different program
    Id.Program programId2 = Id.Program.from("account1", "application1", ProgramType.FLOW, "flow2");
    RunId run4 = RunIds.generate(now - 5000);
    store.setStart(programId2, run4.getId(), runIdToSecs(run4));
    store.setStop(programId2, run4.getId(), nowSecs - 4, ProgramController.State.COMPLETED.getRunStatus());

    // record for different account
    store.setStart(Id.Program.from("account2", "application1", ProgramType.FLOW, "flow1"),
                   run3.getId(), RunIds.getTime(run3, TimeUnit.MILLISECONDS));

    // we should probably be better with "get" method in DefaultStore interface to do that, but we don't have one
    List<RunRecord> successHistory = store.getRuns(programId, ProgramRunStatus.COMPLETED,
                                                   0, Long.MAX_VALUE, Integer.MAX_VALUE);

    List<RunRecord> failureHistory = store.getRuns(programId, ProgramRunStatus.FAILED,
                                                   nowSecs - 20, nowSecs - 10, Integer.MAX_VALUE);
    Assert.assertEquals(failureHistory, store.getRuns(programId, ProgramRunStatus.FAILED,
                                                      0, Long.MAX_VALUE, Integer.MAX_VALUE));

    List<RunRecord> suspendedHistory = store.getRuns(programId, ProgramRunStatus.SUSPENDED,
                                                   nowSecs - 20, nowSecs, Integer.MAX_VALUE);

    // only finished + succeeded runs should be returned
    Assert.assertEquals(1, successHistory.size());
    // only finished + failed runs should be returned
    Assert.assertEquals(1, failureHistory.size());
    // only suspended runs should be returned
    Assert.assertEquals(1, suspendedHistory.size());

    // records should be sorted by start time latest to earliest
    RunRecord run = successHistory.get(0);
    Assert.assertEquals(nowSecs - 10, run.getStartTs());
    Assert.assertEquals(Long.valueOf(nowSecs - 5), run.getStopTs());
    Assert.assertEquals(ProgramController.State.COMPLETED.getRunStatus(), run.getStatus());

    run = failureHistory.get(0);
    Assert.assertEquals(nowSecs - 20, run.getStartTs());
    Assert.assertEquals(Long.valueOf(nowSecs - 10), run.getStopTs());
    Assert.assertEquals(ProgramController.State.ERROR.getRunStatus(), run.getStatus());

    run = suspendedHistory.get(0);
    Assert.assertEquals(run21.getId(), run.getPid());
    Assert.assertEquals(ProgramController.State.SUSPENDED.getRunStatus(), run.getStatus());

    // Assert all history
    List<RunRecord> allHistory = store.getRuns(programId, ProgramRunStatus.ALL,
                                               nowSecs - 20, nowSecs + 1, Integer.MAX_VALUE);
    Assert.assertEquals(allHistory.toString(), 4, allHistory.size());

    // Assert running programs
    List<RunRecord> runningHistory = store.getRuns(programId, ProgramRunStatus.RUNNING, nowSecs, nowSecs + 1, 100);
    Assert.assertEquals(1, runningHistory.size());
    Assert.assertEquals(runningHistory, store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, 100));

    // Get a run record for running program
    RunRecord expectedRunning = runningHistory.get(0);
    Assert.assertNotNull(expectedRunning);
    RunRecord actualRunning = store.getRun(programId, expectedRunning.getPid());
    Assert.assertEquals(expectedRunning, actualRunning);

    // Get a run record for completed run
    RunRecord expectedCompleted = successHistory.get(0);
    Assert.assertNotNull(expectedCompleted);
    RunRecord actualCompleted = store.getRun(programId, expectedCompleted.getPid());
    Assert.assertEquals(expectedCompleted, actualCompleted);

    // Get a run record for suspended run
    RunRecord expectedSuspended = suspendedHistory.get(0);
    Assert.assertNotNull(expectedSuspended);
    RunRecord actualSuspended = store.getRun(programId, expectedSuspended.getPid());
    Assert.assertEquals(expectedSuspended, actualSuspended);

    // Backwards compatibility test with random UUIDs
    // record finished flow
    RunId run5 = RunIds.fromString(UUID.randomUUID().toString());
    store.setStart(programId, run5.getId(), nowSecs - 8);
    store.setStop(programId, run5.getId(), nowSecs - 4, ProgramController.State.COMPLETED.getRunStatus());

    // record not finished flow
    RunId run6 = RunIds.fromString(UUID.randomUUID().toString());
    store.setStart(programId, run6.getId(), nowSecs - 2);

    // Get run record for run5
    RunRecord expectedRecord5 = new RunRecord(run5.getId(), nowSecs - 8, nowSecs - 4, ProgramRunStatus.COMPLETED);
    RunRecord actualRecord5 = store.getRun(programId, run5.getId());
    Assert.assertEquals(expectedRecord5, actualRecord5);

    // Get run record for run6
    RunRecord expectedRecord6 = new RunRecord(run6.getId(), nowSecs - 2, null, ProgramRunStatus.RUNNING);
    RunRecord actualRecord6 = store.getRun(programId, run6.getId());
    Assert.assertEquals(expectedRecord6, actualRecord6);

    // Non-existent run record should give null
    Assert.assertNull(store.getRun(programId, UUID.randomUUID().toString()));

    // Searching for history in wrong time range should give us no results
    Assert.assertTrue(
      store.getRuns(programId, ProgramRunStatus.COMPLETED, nowSecs - 5000, nowSecs - 2000, Integer.MAX_VALUE).isEmpty()
    );
    Assert.assertTrue(
      store.getRuns(programId, ProgramRunStatus.ALL, nowSecs - 5000, nowSecs - 2000, Integer.MAX_VALUE).isEmpty()
    );
  }

  private long runIdToSecs(RunId runId) {
    return RunIds.getTime(runId, TimeUnit.SECONDS);
  }

  @Test
  public void testAddApplication() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Application id = new Id.Application(new Id.Namespace("account1"), "application1");
    store.addApplication(id, spec, new LocalLocationFactory().create("/foo/path/application1.jar"));

    ApplicationSpecification stored = store.getApplication(id);
    assertWordCountAppSpecAndInMetadataStore(stored);

    Assert.assertEquals("/foo/path/application1.jar", store.getApplicationArchiveLocation(id).toURI().getPath());
  }

  @Test
  public void testUpdateSameApplication() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Application id = new Id.Application(new Id.Namespace("account1"), "application1");
    store.addApplication(id, spec, new LocalLocationFactory().create("/foo/path/application1.jar"));
    // update
    store.addApplication(id, spec, new LocalLocationFactory().create("/foo/path/application1_modified.jar"));

    ApplicationSpecification stored = store.getApplication(id);
    assertWordCountAppSpecAndInMetadataStore(stored);
    Assert.assertEquals("/foo/path/application1_modified.jar",
                        store.getApplicationArchiveLocation(id).toURI().getPath());
  }

  @Test
  public void testUpdateChangedApplication() throws Exception {
    Id.Application id = new Id.Application(new Id.Namespace("account1"), "application1");

    store.addApplication(id, Specifications.from(new FooApp()), new LocalLocationFactory().create("/foo"));
    // update
    store.addApplication(id, Specifications.from(new ChangedFooApp()), new LocalLocationFactory().create("/foo"));

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
                    DatasetProperties.builder().add(IndexedTableDefinition.INDEX_COLUMNS_CONF_KEY, "foo").build());
      addFlow(new FlowImpl("flow2"));
      addFlow(new FlowImpl("flow3"));
      addMapReduce(new FooMapReduceJob("mrJob2"));
      addMapReduce(new FooMapReduceJob("mrJob3"));
    }
  }

  private static class FlowImpl implements co.cask.cdap.api.flow.Flow {
    private String name;

    private FlowImpl(String name) {
      this.name = name;
    }

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName(name)
        .setDescription("Flow for counting words")
        .withFlowlets().add(new FlowletImpl("flowlet1"))
        .connect().from(new co.cask.cdap.api.data.stream.Stream("stream1")).to(new FlowletImpl("flowlet1"))
        .build();
    }
  }

  /**
   *
   */
  public static class FlowletImpl extends AbstractFlowlet {
    @UseDataSet("dataset2")
    private KeyValueTable counters;

    @Output("output")
    private OutputEmitter<String> output;

    protected FlowletImpl(String name) {
      super(name);
    }

    @ProcessInput("process")
    public void bar(String str) {
      output.emit(str);
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
    Id.Application appId = new Id.Application(new Id.Namespace(DefaultId.NAMESPACE.getId()), appSpec.getName());
    store.addApplication(appId, appSpec, new LocalLocationFactory().create("/appwithservicestestdelete"));

    AbstractApplication newApp = new AppWithNoServices();

    // get the delete program specs after deploying AppWithNoServices
    List<ProgramSpecification> programSpecs = store.getDeletedProgramSpecifications(appId, Specifications.from(newApp));

    //verify the result.
    Assert.assertEquals(1, programSpecs.size());
    Assert.assertEquals("NoOpService", programSpecs.get(0).getName());
  }

  @Test
  public void testServiceInstances() throws Exception {
    AppFabricTestHelper.deployApplication(AppWithServices.class);
    AbstractApplication app = new AppWithServices();
    DefaultAppConfigurer appConfigurer = new DefaultAppConfigurer(app);
    app.configure(appConfigurer, new ApplicationContext());

    ApplicationSpecification appSpec = appConfigurer.createSpecification();
    Id.Application appId = new Id.Application(new Id.Namespace(DefaultId.NAMESPACE.getId()), appSpec.getName());
    store.addApplication(appId, appSpec, new LocalLocationFactory().create("/appwithservices"));

    // Test setting of service instances
    Id.Program programId = Id.Program.from(appId, ProgramType.SERVICE, "NoOpService");
    int count = store.getServiceInstances(programId);
    Assert.assertEquals(1, count);

    store.setServiceInstances(programId, 10);
    count = store.getServiceInstances(programId);
    Assert.assertEquals(10, count);

    ApplicationSpecification newSpec = store.getApplication(appId);
    Map<String, ServiceSpecification> services = newSpec.getServices();
    Assert.assertEquals(1, services.size());

    ServiceSpecification serviceSpec = services.get("NoOpService");
    Assert.assertEquals(10, serviceSpec.getInstances());
  }

  @Test
  public void testSetFlowletInstances() throws Exception {
    AppFabricTestHelper.deployApplication(WordCountApp.class);

    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    int initialInstances = spec.getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances();
    Id.Application appId = new Id.Application(new Id.Namespace(DefaultId.NAMESPACE.getId()), spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Id.Program programId = new Id.Program(appId, ProgramType.FLOW, "WordCountFlow");
    store.setFlowletInstances(programId, "StreamSource",
                                                      initialInstances + 5);
    // checking that app spec in store was adjusted
    ApplicationSpecification adjustedSpec = store.getApplication(appId);
    Assert.assertEquals(initialInstances + 5,
                        adjustedSpec.getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances());

    // checking that program spec in program jar was adjsuted
    Program program = store.loadProgram(programId, ProgramType.FLOW);
    Assert.assertEquals(initialInstances + 5,
                        program.getApplicationSpecification().
                          getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances());
  }

  @Test
  public void testWorkerInstances() throws Exception {
    AppFabricTestHelper.deployApplication(AppWithWorker.class);
    ApplicationSpecification spec = Specifications.from(new AppWithWorker());

    Id.Application appId = Id.Application.from(DefaultId.NAMESPACE.getId(), spec.getName());
    Id.Program programId = Id.Program.from(appId, ProgramType.WORKER, AppWithWorker.WORKER);

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
    Id.Namespace namespaceId = new Id.Namespace("account1");
    Id.Application appId = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));

    // removing flow
    store.removeAllApplications(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRemoveAll() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Namespace namespaceId = new Id.Namespace("account1");
    Id.Application appId = new Id.Application(namespaceId, "application1");
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));

    // removing flow
    store.removeAll(namespaceId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRemoveApplication() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Namespace namespaceId = new Id.Namespace("account1");
    Id.Application appId = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
  }

  @Test
  public void testRuntimeArgsDeletion() throws Exception {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    Id.Namespace namespaceId = new Id.Namespace("testDeleteRuntimeArgs");
    Id.Application appId = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));

    Id.Program flowProgramId = new Id.Program(appId, ProgramType.FLOW, "NoOpFlow");
    Id.Program mapreduceProgramId = new Id.Program(appId, ProgramType.MAPREDUCE, "NoOpMR");
    Id.Program workflowProgramId = new Id.Program(appId, ProgramType.WORKFLOW, "NoOpWorkflow");

    store.storeRunArguments(flowProgramId, ImmutableMap.of("model", "click"));
    store.storeRunArguments(mapreduceProgramId, ImmutableMap.of("path", "/data"));
    store.storeRunArguments(workflowProgramId, ImmutableMap.of("whitelist", "cask"));


    Map<String, String> args = store.getRunArguments(flowProgramId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("click", args.get("model"));

    args = store.getRunArguments(mapreduceProgramId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("/data", args.get("path"));

    args = store.getRunArguments(workflowProgramId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("cask", args.get("whitelist"));

    // removing application
    store.removeApplication(appId);

    //Check if args are deleted.
    args = store.getRunArguments(flowProgramId);
    Assert.assertEquals(0, args.size());

    args = store.getRunArguments(mapreduceProgramId);
    Assert.assertEquals(0, args.size());

    args = store.getRunArguments(workflowProgramId);
    Assert.assertEquals(0, args.size());
  }

  @Test
  public void testHistoryDeletion() throws Exception {

    // Deploy two apps, write some history for programs
    // Remove application using accountId, AppId and verify
    // Remove all from accountId and verify
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    Id.Namespace namespaceId = new Id.Namespace("testDeleteAll");
    Id.Application appId1 = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId1, spec, new LocalLocationFactory().create("/allPrograms"));

    spec = Specifications.from(new WordCountApp());
    Id.Application appId2 = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId2, spec, new LocalLocationFactory().create("/wordCount"));

    Id.Program flowProgramId1 = new Id.Program(appId1, ProgramType.FLOW, "NoOpFlow");
    Id.Program mapreduceProgramId1 = new Id.Program(appId1, ProgramType.MAPREDUCE, "NoOpMR");
    Id.Program workflowProgramId1 = new Id.Program(appId1, ProgramType.WORKFLOW, "NoOpWorkflow");

    Id.Program flowProgramId2 = new Id.Program(appId2, ProgramType.FLOW, "WordCountFlow");

    Assert.assertNotNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    long now = System.currentTimeMillis();

    store.setStart(flowProgramId1, "flowRun1", now - 1000);
    store.setStop(flowProgramId1, "flowRun1", now, ProgramController.State.COMPLETED.getRunStatus());

    store.setStart(mapreduceProgramId1, "mrRun1", now - 1000);
    store.setStop(mapreduceProgramId1, "mrRun1", now, ProgramController.State.COMPLETED.getRunStatus());

    store.setStart(workflowProgramId1, "wfRun1", now - 1000);
    store.setStop(workflowProgramId1, "wfRun1", now, ProgramController.State.COMPLETED.getRunStatus());

    store.setStart(flowProgramId2, "flowRun2", now - 1000);
    store.setStop(flowProgramId2, "flowRun2", now, ProgramController.State.COMPLETED.getRunStatus());

    verifyRunHistory(flowProgramId1, 1);
    verifyRunHistory(mapreduceProgramId1, 1);
    verifyRunHistory(workflowProgramId1, 1);

    verifyRunHistory(flowProgramId2, 1);

    // removing application
    store.removeApplication(appId1);

    Assert.assertNull(store.getApplication(appId1));

    verifyRunHistory(flowProgramId1, 0);
    verifyRunHistory(mapreduceProgramId1, 0);
    verifyRunHistory(workflowProgramId1, 0);

    // Check to see if the flow history of second app is not deleted
    verifyRunHistory(flowProgramId2, 1);

    // remove all
    store.removeAll(namespaceId);

    verifyRunHistory(flowProgramId2, 0);
  }

  private void verifyRunHistory(Id.Program programId, int count) {
    List<RunRecord> history = store.getRuns(programId, ProgramRunStatus.ALL,
                                            0, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(count, history.size());
  }

  @Test
  public void testCheckDeletedProgramSpecs () throws Exception {
    //Deploy program with all types of programs.
    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Set<String> specsToBeVerified = Sets.newHashSet();
    specsToBeVerified.addAll(spec.getMapReduce().keySet());
    specsToBeVerified.addAll(spec.getWorkflows().keySet());
    specsToBeVerified.addAll(spec.getFlows().keySet());

    //Verify if there are 4 program specs in AllProgramsApp
    Assert.assertEquals(3, specsToBeVerified.size());

    Id.Application appId = Id.Application.from(DefaultId.NAMESPACE, "App");
    // Check the diff with the same app - re-deployement scenario where programs are not removed.
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId,  spec);
    Assert.assertEquals(0, deletedSpecs.size());

    //Get the spec for app that contains no programs.
    spec = Specifications.from(new NoProgramsApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(3, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeVerified.remove(specification.getName());
    }

    //All the 4 specs should have been deleted.
    Assert.assertEquals(0, specsToBeVerified.size());
  }

  @Test
  public void testCheckDeletedWorkflow () throws Exception {
    //Deploy program with all types of programs.
    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Set<String> specsToBeDeleted = Sets.newHashSet();
    specsToBeDeleted.addAll(spec.getWorkflows().keySet());

    Assert.assertEquals(1, specsToBeDeleted.size());

    Id.Application appId = Id.Application.from(DefaultId.NAMESPACE, "App");

    //Get the spec for app that contains only flow and mapreduce - removing workflows.
    spec = Specifications.from(new FlowMapReduceApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(1, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeDeleted.remove(specification.getName());
    }

    //2 specs should have been deleted and 0 should be remaining.
    Assert.assertEquals(0, specsToBeDeleted.size());
  }

  private static final Id.Namespace account = new Id.Namespace(Constants.DEFAULT_NAMESPACE);
  private static final Id.Application appId = new Id.Application(account, AppWithWorkflow.NAME);
  private static final Id.Program program = new Id.Program(appId, ProgramType.WORKFLOW,
                                                           AppWithWorkflow.SampleWorkflow.NAME);
  private static final SchedulableProgramType programType = SchedulableProgramType.WORKFLOW;
  private static final Schedule schedule1 = Schedules.createTimeSchedule("Schedule1", "Every minute", "* * * * ?");
  private static final Schedule schedule2 = Schedules.createTimeSchedule("Schedule2", "Every Hour", "0 * * * ?");
  private static final Schedule scheduleWithSameName = Schedules.createTimeSchedule("Schedule2", "Every minute",
                                                                                    "* * * * ?");
  private static final Map<String, String> properties1 = ImmutableMap.of();
  private static final Map<String, String> properties2 = ImmutableMap.of();
  private static final ScheduleSpecification scheduleSpec1 =
    new ScheduleSpecification(schedule1, new ScheduleProgramInfo(programType, AppWithWorkflow.SampleWorkflow.NAME),
                              properties1);
  private static final ScheduleSpecification scheduleSpec2 =
    new ScheduleSpecification(schedule2, new ScheduleProgramInfo(programType, AppWithWorkflow.SampleWorkflow.NAME),
                              properties2);
  private static final ScheduleSpecification scheduleWithSameNameSpec =
    new ScheduleSpecification(scheduleWithSameName, new ScheduleProgramInfo(programType,
                                                                            AppWithWorkflow.SampleWorkflow.NAME),
                              properties2);

  @Test
  public void testDynamicScheduling() throws Exception {
    AppFabricTestHelper.deployApplication(AppWithWorkflow.class);
    Id.Application appId = Id.Application.from(Constants.DEFAULT_NAMESPACE, AppWithWorkflow.NAME);

    Map<String, ScheduleSpecification> schedules = getSchedules(appId);
    Assert.assertEquals(0, schedules.size());

    store.addSchedule(program, scheduleSpec1);
    schedules = getSchedules(appId);
    Assert.assertEquals(1, schedules.size());
    Assert.assertEquals(scheduleSpec1, schedules.get("Schedule1"));

    store.addSchedule(program, scheduleSpec2);
    schedules = getSchedules(appId);
    Assert.assertEquals(2, schedules.size());
    Assert.assertEquals(scheduleSpec2, schedules.get("Schedule2"));

    try {
      store.addSchedule(program, scheduleWithSameNameSpec);
      Assert.fail("Should have thrown Exception because multiple schedules with the same name are being added.");
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getCause().getMessage(),
                          "Schedule with the name 'Schedule2' already exists.");
    }

    store.deleteSchedule(program, programType, "Schedule2");
    schedules = getSchedules(appId);
    Assert.assertEquals(1, schedules.size());
    Assert.assertEquals(null, schedules.get("Schedule2"));

    try {
      store.deleteSchedule(program, programType, "Schedule2");
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(NoSuchElementException.class, Throwables.getRootCause(e).getClass());
    }
    schedules = getSchedules(appId);
    Assert.assertEquals(1, schedules.size());
    Assert.assertEquals(null, schedules.get("Schedule2"));
  }

  private Map<String, ScheduleSpecification> getSchedules(Id.Application appId) {
    ApplicationSpecification application = store.getApplication(appId);
    Assert.assertNotNull(application);
    return application.getSchedules();
  }

  @Test
  public void testAdapterMDSOperations() throws Exception {
    Id.Namespace namespaceId = new Id.Namespace("testAdapterMDS");

    AdapterDefinition spec1 = AdapterDefinition.builder("spec1", Id.Program.from(namespaceId,
                                                                                 "template1",
                                                                                 ProgramType.WORKFLOW,
                                                                                 "program1"))
      .setConfig(GSON.toJsonTree(ImmutableMap.of("k1", "v1")).getAsJsonObject())
      .build();

    TemplateConf templateConf = new TemplateConf(5, "5", ImmutableMap.of("123", "456"));
    AdapterDefinition spec2 = AdapterDefinition.builder("spec2", Id.Program.from(namespaceId, "template2",
                                                                                 ProgramType.WORKER, "program2"))
      .setConfig(GSON.toJsonTree(templateConf).getAsJsonObject())
      .build();

    store.addAdapter(namespaceId, spec1);
    store.addAdapter(namespaceId, spec2);

    // check get all adapters
    Collection<AdapterDefinition> adapters = store.getAllAdapters(namespaceId);
    Assert.assertEquals(2, adapters.size());
    // apparently JsonObjects can be equal, but have different hash codes which means we can't just put
    // them in a set and compare...
    Iterator<AdapterDefinition> iter = adapters.iterator();
    AdapterDefinition actual1 = iter.next();
    AdapterDefinition actual2 = iter.next();
    // since order is not guaranteed...
    if (actual1.getName().equals(spec1.getName())) {
      Assert.assertEquals(actual1, spec1);
      Assert.assertEquals(actual2, spec2);
    } else {
      Assert.assertEquals(actual1, spec2);
      Assert.assertEquals(actual2, spec1);
    }

    // Get non existing spec
    AdapterDefinition retrievedAdapter = store.getAdapter(namespaceId, "nonExistingAdapter");
    Assert.assertNull(retrievedAdapter);

    //Retrieve specs
    AdapterDefinition retrievedSpec1 = store.getAdapter(namespaceId, spec1.getName());
    Assert.assertEquals(spec1, retrievedSpec1);
    // Remove spec
    store.removeAdapter(namespaceId, spec1.getName());

    // verify the deleted spec is gone.
    retrievedAdapter = store.getAdapter(namespaceId, spec1.getName());
    Assert.assertNull(retrievedAdapter);

    // verify the other adapter still exists
    AdapterDefinition retrievedSpec2 = store.getAdapter(namespaceId, spec2.getName());
    Assert.assertEquals(spec2, retrievedSpec2);

    // remove all
    store.removeAllAdapters(namespaceId);

    // verify all adapters are gone
    retrievedAdapter = store.getAdapter(namespaceId, spec2.getName());
    Assert.assertNull(retrievedAdapter);
  }

  private static class TemplateConf {
    private final int x;
    private final String y;
    private final Map<String, String> z;

    public TemplateConf(int x, String y, Map<String, String> z) {
      this.x = x;
      this.y = y;
      this.z = z;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TemplateConf that = (TemplateConf) o;

      return Objects.equal(x, that.x) && Objects.equal(y, that.y) && Objects.equal(z, that.z);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(x, y, z);
    }
  }
}
