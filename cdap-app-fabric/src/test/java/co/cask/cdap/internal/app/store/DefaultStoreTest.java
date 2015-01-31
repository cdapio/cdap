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
import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.FlowMapReduceApp;
import co.cask.cdap.NoProgramsApp;
import co.cask.cdap.ToyApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.annotation.Handle;
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
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.Specifications;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.internal.DefaultId;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 */
public class DefaultStoreTest {
  private static DefaultStore store;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    store = injector.getInstance(DefaultStore.class);
  }

  @Before
  public void before() throws Exception {
    store.clear();
    // The store.clear() call above deletes the 'default' namespace, which causes subsequent tests to fail.
    // The default namespace is only created during AppFabricServer (NamespaceService in particular) startup.
    // Re-creating it once here temporarily until AppFabricTestHelper uses v2 APIs for managing app lifecycle.
    // In a real-scenario (non-unit test), users are not allowed to delete the 'default' namespace, so this scenario
    // should never occur.
    // Since this is only temporary and is for unit test purposes (and is the unit test of Store), using the Store
    // directly to re-create the default namespace rather than restarting/re-initializing NamespaceService.
    // TODO: CDAP-1368: Update AppFabricTestHelper to use v3 APIs, and create/use non-default namespaces in these tests.
    store.createNamespace(new NamespaceMeta.Builder()
                            .setId(Constants.DEFAULT_NAMESPACE)
                            .setName(Constants.DEFAULT_NAMESPACE)
                            .setDescription(Constants.DEFAULT_NAMESPACE)
                            .build());
  }

  @Test
  public void testLoadingProgram() throws Exception {
    AppFabricTestHelper.deployApplication(ToyApp.class);
    Program program = store.loadProgram(Id.Program.from(DefaultId.NAMESPACE.getId(), "ToyApp", "ToyFlow"),
                                        ProgramType.FLOW);
    Assert.assertNotNull(program);
  }

  @Test(expected = RuntimeException.class)
  public void testStopBeforeStart() throws RuntimeException {
    Id.Program programId = Id.Program.from("account1", "invalidApp", "InvalidFlowOperation");
    long now = System.currentTimeMillis();
    store.setStop(programId, "runx", now, ProgramController.State.ERROR);
  }

  @Test
  public void testConcurrentStopStart() throws Exception {
    // Two programs that start/stop at same time
    // Should have two run history.
    Id.Program programId = Id.Program.from("account1", "concurrentApp", "concurrentFlow");
    long now = System.currentTimeMillis();

    store.setStart(programId, "run1", now - 1000);
    store.setStart(programId, "run2", now - 1000);

    store.setStop(programId, "run1", now, ProgramController.State.STOPPED);
    store.setStop(programId, "run2", now, ProgramController.State.STOPPED);

    List<RunRecord> history = store.getRuns(programId, ProgramRunStatus.ALL,
                                            Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(2, history.size());
  }

  @Test
  public void testLogProgramRunHistory() throws Exception {
    // record finished flow
    Id.Program programId = Id.Program.from("account1", "application1", "flow1");
    long now = System.currentTimeMillis();

    store.setStart(programId, "run1", now - 2000);
    store.setStop(programId, "run1", now - 1000, ProgramController.State.ERROR);

    // record another finished flow
    store.setStart(programId, "run2", now - 1000);
    store.setStop(programId, "run2", now - 500, ProgramController.State.STOPPED);

    // record not finished flow
    store.setStart(programId, "run3", now);

    // record run of different program
    Id.Program programId2 = Id.Program.from("account1", "application1", "flow2");
    store.setStart(programId2, "run4", now - 500);
    store.setStop(programId2, "run4", now - 400, ProgramController.State.STOPPED);

    // record for different account
    store.setStart(Id.Program.from("account2", "application1", "flow1"), "run3", now - 300);

    // we should probably be better with "get" method in DefaultStore interface to do that, but we don't have one
    List<RunRecord> successHistory = store.getRuns(programId, ProgramRunStatus.COMPLETED,
                                                   Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);

    List<RunRecord> failureHistory = store.getRuns(programId, ProgramRunStatus.FAILED,
                                                   Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);

    // only finished + succeeded runs should be returned
    Assert.assertEquals(1, successHistory.size());
    // only finished + failed runs should be returned
    Assert.assertEquals(1, failureHistory.size());
    // records should be sorted by start time latest to earliest
    RunRecord run = successHistory.get(0);
    Assert.assertEquals(now - 1000, run.getStartTs());
    Assert.assertEquals(now - 500, run.getStopTs());
    Assert.assertEquals(ProgramController.State.STOPPED.getRunStatus(), run.getStatus());

    run = failureHistory.get(0);
    Assert.assertEquals(now - 2000, run.getStartTs());
    Assert.assertEquals(now - 1000, run.getStopTs());
    Assert.assertEquals(ProgramController.State.ERROR.getRunStatus(), run.getStatus());
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
      addProcedure(new ProcedureImpl("procedure1"));
      addProcedure(new ProcedureImpl("procedure2"));
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
      addProcedure(new ProcedureImpl("procedure2"));
      addProcedure(new ProcedureImpl("procedure3"));
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

  /**
   *
   */
  public static class ProcedureImpl extends AbstractProcedure {
    @UseDataSet("dataset2")
    private KeyValueTable counters;

    protected ProcedureImpl(String name) {
      super(name);
    }

    @Handle("proced")
    public void process(String word) throws Exception {
      this.counters.read(word.getBytes(Charsets.UTF_8));
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
    Id.Program programId = Id.Program.from(appId, "NoOpService");
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

    Id.Program programId = new Id.Program(appId, "WordCountFlow");
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
  public void testProcedureInstances() throws Exception {

    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Id.Application appId = new Id.Application(new Id.Namespace(DefaultId.NAMESPACE.getId()), spec.getName());
    Id.Program programId = new Id.Program(appId, "NoOpProcedure");

    int instancesFromSpec = spec.getProcedures().get("NoOpProcedure").getInstances();
    Assert.assertEquals(1, instancesFromSpec);
    int instances = store.getProcedureInstances(programId);
    Assert.assertEquals(instancesFromSpec, instances);

    store.setProcedureInstances(programId, 10);
    instances = store.getProcedureInstances(programId);
    Assert.assertEquals(10, instances);
  }

  @Test
  public void testRemoveAllApplications() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Namespace namespaceId = new Id.Namespace("account1");
    Id.Application appId = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertEquals(1, store.getAllStreams(new Id.Namespace("account1")).size());

    // removing flow
    store.removeAllApplications(namespaceId);

    Assert.assertNull(store.getApplication(appId));
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, store.getAllStreams(new Id.Namespace("account1")).size());
  }

  @Test
  public void testRemoveAll() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Namespace namespaceId = new Id.Namespace("account1");
    Id.Application appId = new Id.Application(namespaceId, "application1");
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertEquals(1, store.getAllStreams(new Id.Namespace("account1")).size());

    // removing flow
    store.removeAll(namespaceId);

    Assert.assertNull(store.getApplication(appId));
    // Streams and DataSets should not survive deletion
    Assert.assertEquals(0, store.getAllStreams(new Id.Namespace("account1")).size());
  }

  @Test
  public void testRemoveApplication() throws Exception {
    ApplicationSpecification spec = Specifications.from(new WordCountApp());
    Id.Namespace namespaceId = new Id.Namespace("account1");
    Id.Application appId = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertEquals(1, store.getAllStreams(new Id.Namespace("account1")).size());

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, store.getAllStreams(new Id.Namespace("account1")).size());
  }

  @Test
  public void testRuntimeArgsDeletion() throws Exception {
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());
    Id.Namespace namespaceId = new Id.Namespace("testDeleteRuntimeArgs");
    Id.Application appId = new Id.Application(namespaceId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));

    Id.Program flowProgramId = new Id.Program(appId, "NoOpFlow");
    Id.Program mapreduceProgramId = new Id.Program(appId, "NoOpMR");
    Id.Program procedureProgramId = new Id.Program(appId, "NoOpProcedure");
    Id.Program workflowProgramId = new Id.Program(appId, "NoOpWorkflow");

    store.storeRunArguments(flowProgramId, ImmutableMap.of("model", "click"));
    store.storeRunArguments(mapreduceProgramId, ImmutableMap.of("path", "/data"));
    store.storeRunArguments(procedureProgramId, ImmutableMap.of("timeoutMs", "1000"));
    store.storeRunArguments(workflowProgramId, ImmutableMap.of("whitelist", "cask"));


    Map<String, String> args = store.getRunArguments(flowProgramId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("click", args.get("model"));

    args = store.getRunArguments(mapreduceProgramId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("/data", args.get("path"));

    args = store.getRunArguments(procedureProgramId);
    Assert.assertEquals(1, args.size());
    Assert.assertEquals("1000", args.get("timeoutMs"));

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

    args = store.getRunArguments(procedureProgramId);
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

    Id.Program flowProgramId1 = new Id.Program(appId1, "NoOpFlow");
    Id.Program mapreduceProgramId1 = new Id.Program(appId1, "NoOpMR");
    Id.Program procedureProgramId1 = new Id.Program(appId1, "NoOpProcedure");
    Id.Program workflowProgramId1 = new Id.Program(appId1, "NoOpWorkflow");

    Id.Program flowProgramId2 = new Id.Program(appId2, "WordCountFlow");

    Assert.assertNotNull(store.getApplication(appId1));
    Assert.assertNotNull(store.getApplication(appId2));

    long now = System.currentTimeMillis();

    store.setStart(flowProgramId1, "flowRun1", now - 1000);
    store.setStop(flowProgramId1, "flowRun1", now, ProgramController.State.STOPPED);

    store.setStart(mapreduceProgramId1, "mrRun1", now - 1000);
    store.setStop(mapreduceProgramId1, "mrRun1", now, ProgramController.State.STOPPED);

    store.setStart(procedureProgramId1, "procedureRun1", now - 1000);
    store.setStop(procedureProgramId1, "procedureRun1", now, ProgramController.State.STOPPED);

    store.setStart(workflowProgramId1, "wfRun1", now - 1000);
    store.setStop(workflowProgramId1, "wfRun1", now, ProgramController.State.STOPPED);

    store.setStart(flowProgramId2, "flowRun2", now - 1000);
    store.setStop(flowProgramId2, "flowRun2", now, ProgramController.State.STOPPED);

    verifyRunHistory(flowProgramId1, 1);
    verifyRunHistory(mapreduceProgramId1, 1);
    verifyRunHistory(procedureProgramId1, 1);
    verifyRunHistory(workflowProgramId1, 1);

    verifyRunHistory(flowProgramId2, 1);

    // removing application
    store.removeApplication(appId1);

    Assert.assertNull(store.getApplication(appId1));

    verifyRunHistory(flowProgramId1, 0);
    verifyRunHistory(mapreduceProgramId1, 0);
    verifyRunHistory(procedureProgramId1, 0);
    verifyRunHistory(workflowProgramId1, 0);

    // Check to see if the flow history of second app is not deleted
    verifyRunHistory(flowProgramId2, 1);

    // remove all
    store.removeAll(namespaceId);

    verifyRunHistory(flowProgramId2, 0);
  }

  private void verifyRunHistory(Id.Program programId, int count) {
    List<RunRecord> history = store.getRuns(programId, ProgramRunStatus.ALL,
                                            Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(count, history.size());
  }

  @Test
  public void testCheckDeletedProgramSpecs () throws Exception {
    //Deploy program with all types of programs.
    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Set<String> specsToBeVerified = Sets.newHashSet();
    specsToBeVerified.addAll(spec.getProcedures().keySet());
    specsToBeVerified.addAll(spec.getMapReduce().keySet());
    specsToBeVerified.addAll(spec.getWorkflows().keySet());
    specsToBeVerified.addAll(spec.getFlows().keySet());

    //Verify if there are 4 program specs in AllProgramsApp
    Assert.assertEquals(4, specsToBeVerified.size());

    Id.Application appId = Id.Application.from(DefaultId.NAMESPACE, "App");
    // Check the diff with the same app - re-deployement scenario where programs are not removed.
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId,  spec);
    Assert.assertEquals(0, deletedSpecs.size());

    //Get the spec for app that contains no programs.
    spec = Specifications.from(new NoProgramsApp());

    //Get the deleted program specs by sending a spec with same name as AllProgramsApp but with no programs
    deletedSpecs = store.getDeletedProgramSpecifications(appId, spec);
    Assert.assertEquals(4, deletedSpecs.size());

    for (ProgramSpecification specification : deletedSpecs) {
      //Remove the spec that is verified, to check the count later.
      specsToBeVerified.remove(specification.getName());
    }

    //All the 4 specs should have been deleted.
    Assert.assertEquals(0, specsToBeVerified.size());
  }

  @Test
  public void testCheckDeletedProceduresAndWorkflow () throws Exception {
    //Deploy program with all types of programs.
    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = Specifications.from(new AllProgramsApp());

    Set<String> specsToBeDeleted = Sets.newHashSet();
    specsToBeDeleted.addAll(spec.getWorkflows().keySet());
    specsToBeDeleted.addAll(spec.getProcedures().keySet());

    Assert.assertEquals(2, specsToBeDeleted.size());

    Id.Application appId = Id.Application.from(DefaultId.NAMESPACE, "App");

    //Get the spec for app that contains only flow and mapreduce - removing procedures and workflows.
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

  private static final Id.Namespace account = new Id.Namespace(Constants.DEFAULT_NAMESPACE);
  private static final Id.Application appId = new Id.Application(account, AppWithWorkflow.NAME);
  private static final Id.Program program = new Id.Program(appId, AppWithWorkflow.SampleWorkflow.NAME);
  private static final SchedulableProgramType programType = SchedulableProgramType.WORKFLOW;
  private static final Schedule schedule1 = new Schedule("Schedule1", "Every minute", "* * * * ?");
  private static final Schedule schedule2 = new Schedule("Schedule2", "Every Hour", "0 * * * ?");
  private static final Schedule scheduleWithSameName = new Schedule("Schedule2", "Every minute", "* * * * ?");
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

    Map<String, String> properties = ImmutableMap.of("frequency", "10m");
    Set<Source> sources = Sets.newHashSet(new Source("eventStream", Source.Type.STREAM,
                                                         ImmutableMap.of("prop1", "val1")));

    Set<Sink> sinks = Sets.newHashSet(new Sink("myAvroFiles", Sink.Type.DATASET,
                                                   ImmutableMap.of("type", "co.cask.cdap.data.dataset.Fileset")));

    AdapterSpecification specStreamToAvro1 = new AdapterSpecification("streamToAvro1", "batchStreamToAvro",
                                                                     properties, sources, sinks);

    AdapterSpecification specStreamToAvro2 = new AdapterSpecification("streamToAvro2", "batchStreamToAvro",
                                                                     properties, sources, sinks);

    store.addAdapter(namespaceId, specStreamToAvro1);
    store.addAdapter(namespaceId, specStreamToAvro2);

    // Get non existing spec
    AdapterSpecification retrievedAdapter = store.getAdapter(namespaceId, "nonExistingAdapter");
    Assert.assertNull(retrievedAdapter);

    //Retrieve specs
    AdapterSpecification retrievedSpec = store.getAdapter(namespaceId, "streamToAvro1");
    Assert.assertEquals(specStreamToAvro1, retrievedSpec);
    // Remove spec
    store.removeAdapter(namespaceId, "streamToAvro1");

    // verify the deleted spec is gone.
    retrievedAdapter = store.getAdapter(namespaceId, "streamToAvro1");
    Assert.assertNull(retrievedAdapter);

    // verify the other adapter still exists
    retrievedSpec = store.getAdapter(namespaceId, "streamToAvro2");
    Assert.assertEquals(specStreamToAvro2, retrievedSpec);

    // remove all
    store.removeAllAdapters(namespaceId);

    // verify all adapters are gone
    retrievedAdapter = store.getAdapter(namespaceId, "streamToAvro2");
    Assert.assertNull(retrievedAdapter);
  }
}
