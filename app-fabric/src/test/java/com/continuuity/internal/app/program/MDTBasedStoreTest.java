/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.AllProgramsApp;
import com.continuuity.AppWithNoServices;
import com.continuuity.AppWithServices;
import com.continuuity.FlowMapReduceApp;
import com.continuuity.NoProgramsApp;
import com.continuuity.ToyApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.app.Application;
import com.continuuity.api.app.ApplicationContext;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.DefaultAppConfigurer;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.app.store.MDTBasedStore;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.continuuity.test.internal.DefaultId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class MDTBasedStoreTest {
  private MDTBasedStore store;

  // we do it in @Before (not in @BeforeClass) to have easy automatic cleanup between tests
  @Before
  public void before() throws OperationException {
    store = AppFabricTestHelper.getInjector().getInstance(MDTBasedStore.class);

    // clean up data
    MetaDataTable mds = AppFabricTestHelper.getInjector().getInstance(MetaDataTable.class);
    for (String account : mds.listAccounts(new OperationContext(DefaultId.DEFAULT_ACCOUNT_ID))) {
      mds.clear(new OperationContext(account), account, null);
    }
  }

  @Test
  public void testLoadingProgram() throws Exception {
    AppFabricTestHelper.deployApplication(ToyApp.class);
    Program program = store.loadProgram(Id.Program.from(DefaultId.ACCOUNT.getId(), "ToyApp", "ToyFlow"), Type.FLOW);
    Assert.assertNotNull(program);
  }

  @Test(expected = RuntimeException.class)
  public void testStopBeforeStart() throws RuntimeException {
    Id.Program programId = Id.Program.from("account1", "invalidApp", "InvalidFlowOperation");
    long now = System.currentTimeMillis();
    store.setStop(programId, "runx", now, "FAILED");
  }

  @Test
  public void testConcurrentStopStart() throws OperationException {
    // Two programs that start/stop at same time
    // Should have two run history.
    Id.Program programId = Id.Program.from("account1", "concurrentApp", "concurrentFlow");
    long now = System.currentTimeMillis();

    store.setStart(programId, "run1", now - 1000);
    store.setStart(programId, "run2", now - 1000);

    store.setStop(programId, "run1", now, "SUCCEDED");
    store.setStop(programId, "run2", now, "SUCCEDED");

    List<RunRecord> history = store.getRunHistory(programId, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(2, history.size());
  }

  @Test
  public void testLogProgramRunHistory() throws OperationException {
    // record finished flow
    Id.Program programId = Id.Program.from("account1", "application1", "flow1");
    long now = System.currentTimeMillis();

    store.setStart(programId, "run1", now - 2000);
    store.setStop(programId, "run1", now - 1000, "FAILED");

    // record another finished flow
    store.setStart(programId, "run2", now - 1000);
    store.setStop(programId, "run2", now - 500, "SUCCEEDED");

    // record not finished flow
    store.setStart(programId, "run3", now);

    // record run of different program
    Id.Program programId2 = Id.Program.from("account1", "application1", "flow2");
    store.setStart(programId2, "run4", now - 500);
    store.setStop(programId2, "run4", now - 400, "SUCCEEDED");

    // record for different account
    store.setStart(Id.Program.from("account2", "application1", "flow1"), "run3", now - 300);

    // we should probably be better with "get" method in MDTBasedStore interface to do that, but we don't have one
    List<RunRecord> history = store.getRunHistory(programId, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);

    // only finished runs should be returned
    Assert.assertEquals(2, history.size());
    // records should be sorted by start time latest to earliest
    RunRecord run = history.get(0);
    Assert.assertEquals(now - 1000, run.getStartTs());
    Assert.assertEquals(now - 500, run.getStopTs());
    Assert.assertEquals("SUCCEEDED", run.getEndStatus());

    run = history.get(1);
    Assert.assertEquals(now - 2000, run.getStartTs());
    Assert.assertEquals(now - 1000, run.getStopTs());
    Assert.assertEquals("FAILED", run.getEndStatus());

    // testing "get all history for account"
    // note: we need to add account's apps info into store
    store.addApplication(Id.Application.from("account1", "application1"),
                         getSpec(
                           com.continuuity.api.ApplicationSpecification.Builder.with()
                             .setName("application1").setDescription("")
                             .noStream().noDataSet()
                             .withFlows().add(new FlowImpl("flow1")).add(new FlowImpl("flow2"))
                             .noProcedure().noMapReduce().noWorkflow().build()),
                         new LocalLocationFactory().create("/foo"));
    store.addApplication(Id.Application.from("account2", "application1"),
                         getSpec(
                           com.continuuity.api.ApplicationSpecification.Builder.with()
                             .setName("application1").setDescription("")
                             .noStream().noDataSet()
                             .withFlows().add(new FlowImpl("flow1")).add(new FlowImpl("flow2"))
                             .noProcedure().noMapReduce().noWorkflow().build()),
                         new LocalLocationFactory().create("/foo"));

    com.google.common.collect.Table<Type, Id.Program, List<RunRecord>> runHistory =
                                                           store.getAllRunHistory(new Id.Account("account1"));

    // we ran two programs (flows)
    Assert.assertEquals(2, runHistory.size());
    int totalHistoryRecords = 0;
    for (com.google.common.collect.Table.Cell<Type, Id.Program, List<RunRecord>> cell : runHistory.cellSet()) {
      totalHistoryRecords += cell.getValue().size();
    }
    // there were 3 "finished" runs of different programs
    Assert.assertEquals(3, totalHistoryRecords);
  }

  @Test
  public void testAddApplication() throws Exception {
    ApplicationSpecification spec = getSpec(new WordCountApp().configure());
    Id.Application id = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(id, spec, new LocalLocationFactory().create("/foo/path/application1.jar"));

    ApplicationSpecification stored = store.getApplication(id);
    assertWordCountAppSpecAndInMetadataStore(stored);

    Assert.assertEquals("/foo/path/application1.jar", store.getApplicationArchiveLocation(id).toURI().getPath());
  }

  @Test
  public void testUpdateSameApplication() throws Exception {
    ApplicationSpecification spec = getSpec(new WordCountApp().configure());
    Id.Application id = new Id.Application(new Id.Account("account1"), "application1");
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
    Id.Application id = new Id.Application(new Id.Account("account1"), "application1");

    store.addApplication(id, getSpec(new FooApp().configure()), new LocalLocationFactory().create("/foo"));
    // update
    store.addApplication(id, getSpec(new ChangedFooApp().configure()), new LocalLocationFactory().create("/foo"));

    ApplicationSpecification stored = store.getApplication(id);
    assertChangedFooAppSpecAndInMetadataStore(stored);
  }

  private ApplicationSpecification getSpec(com.continuuity.api.ApplicationSpecification spec) {
    return Specifications.from(spec);
  }

  private static class FooApp implements com.continuuity.api.Application {
    @Override
    public com.continuuity.api.ApplicationSpecification configure() {
      return com.continuuity.api.ApplicationSpecification.Builder.with()
        .setName("FooApp")
        .setDescription("Foo App")
        .withStreams()
          .add(new com.continuuity.api.data.stream.Stream("stream1"))
          .add(new com.continuuity.api.data.stream.Stream("stream2"))
        .withDataSets()
          .add(new Table("dataset1"))
          .add(new KeyValueTable("dataset2"))
        .withFlows()
          .add(new FlowImpl("flow1"))
          .add(new FlowImpl("flow2"))
        .withProcedures()
          .add(new ProcedureImpl("procedure1"))
          .add(new ProcedureImpl("procedure2"))
        .withMapReduce()
          .add(new FooMapReduceJob("mrJob1"))
          .add(new FooMapReduceJob("mrJob2"))
        .noWorkflow()
        .build();
    }
  }

  private static class ChangedFooApp implements com.continuuity.api.Application {
    @Override
    public com.continuuity.api.ApplicationSpecification configure() {
      return com.continuuity.api.ApplicationSpecification.Builder.with()
        .setName("FooApp")
        .setDescription("Foo App")
        .withStreams()
          .add(new com.continuuity.api.data.stream.Stream("stream2"))
          .add(new com.continuuity.api.data.stream.Stream("stream3"))
        .withDataSets()
          .add(new KeyValueTable("dataset2"))
          .add(new IndexedTable("dataset3", Bytes.toBytes("foo")))
        .withFlows()
          .add(new FlowImpl("flow2"))
          .add(new FlowImpl("flow3"))
        .withProcedures()
          .add(new ProcedureImpl("procedure2"))
          .add(new ProcedureImpl("procedure3"))
        .withMapReduce()
          .add(new FooMapReduceJob("mrJob2"))
          .add(new FooMapReduceJob("mrJob3"))
        .noWorkflow()
        .build();
    }
  }

  private static class FlowImpl implements com.continuuity.api.flow.Flow {
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
        .connect().from(new com.continuuity.api.data.stream.Stream("stream1")).to(new FlowletImpl("flowlet1"))
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
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName(name)
        .setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing MDS")
        .build();
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
    public void process(String word) throws OperationException {
      this.counters.read(word.getBytes(Charsets.UTF_8));
    }
  }


  private void assertWordCountAppSpecAndInMetadataStore(ApplicationSpecification stored) {
    // should be enough to make sure it is stored
    Assert.assertEquals(1, stored.getDataSets().size());
    Assert.assertEquals(WordCountApp.WordCountFlow.class.getName(),
                        stored.getFlows().get("WordCountFlow").getClassName());
  }

  private void assertChangedFooAppSpecAndInMetadataStore(ApplicationSpecification stored) {
    // should be enough to make sure it is stored
    Assert.assertEquals(2, stored.getDataSets().size());
    Assert.assertEquals(FlowImpl.class.getName(),
                        stored.getFlows().get("flow2").getClassName());
  }


  @Test
  public void testServiceDeletion() throws Exception {
    // Store the application specification
    AbstractApplication app = new AppWithServices();

    ApplicationSpecification appSpec = getAppSpec(app);
    Id.Application appId = new Id.Application(new Id.Account(DefaultId.ACCOUNT.getId()), appSpec.getName());
    store.addApplication(appId, appSpec, new LocalLocationFactory().create("/appwithservicestestdelete"));

    AbstractApplication newApp = new AppWithNoServices();

    // get the delete program specs after deploying AppWithNoServices
    List<ProgramSpecification> programSpecs = store.getDeletedProgramSpecifications(appId, getAppSpec(newApp));

    //verify the result.
    Assert.assertEquals(1, programSpecs.size());
    Assert.assertEquals("NoOpService", programSpecs.get(0).getName());
  }

  private ApplicationSpecification getAppSpec(Application application) {
    DefaultAppConfigurer appConfigurer = new DefaultAppConfigurer(application);
    application.configure(appConfigurer, new ApplicationContext());
    return appConfigurer.createApplicationSpec();
  }

  @Test
  public void testServiceRunnableInstances() throws Exception {
    AppFabricTestHelper.deployApplication(AppWithServices.class);
    AbstractApplication app = new AppWithServices();
    DefaultAppConfigurer appConfigurer = new DefaultAppConfigurer(app);
    app.configure(appConfigurer, new ApplicationContext());

    ApplicationSpecification appSpec = appConfigurer.createApplicationSpec();
    Id.Application appId = new Id.Application(new Id.Account(DefaultId.ACCOUNT.getId()), appSpec.getName());
    store.addApplication(appId, appSpec, new LocalLocationFactory().create("/appwithservices"));

    Id.Program programId = Id.Program.from(appId, "NoOpService");
    int count = store.getServiceRunnableInstances(programId, "DummyService");
    Assert.assertEquals(1, count);

    store.setServiceRunnableInstances(programId, "DummyService", 10);
    count = store.getServiceRunnableInstances(programId, "DummyService");
    Assert.assertEquals(10, count);

    ApplicationSpecification newSpec = store.getApplication(appId);
    Map<String, ServiceSpecification> services = newSpec.getServices();
    Assert.assertEquals(1, services.size());

    Map<String, RuntimeSpecification> runtimeSpecs = services.get("NoOpService").getRunnables();
    Assert.assertEquals(1, runtimeSpecs.size());
    Assert.assertEquals(10, runtimeSpecs.get("DummyService").getResourceSpecification().getInstances());
  }

  @Test
  public void testSetFlowletInstances() throws Exception {
    AppFabricTestHelper.deployApplication(WordCountApp.class);

    ApplicationSpecification spec = getSpec(new WordCountApp().configure());
    int initialInstances = spec.getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances();
    Id.Application appId = new Id.Application(new Id.Account(DefaultId.ACCOUNT.getId()), spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Id.Program programId = new Id.Program(appId, "WordCountFlow");
    store.setFlowletInstances(programId, "StreamSource",
                                                      initialInstances + 5);
    // checking that app spec in store was adjusted
    ApplicationSpecification adjustedSpec = store.getApplication(appId);
    Assert.assertEquals(initialInstances + 5,
                        adjustedSpec.getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances());

    // checking that program spec in program jar was adjsuted
    Program program = store.loadProgram(programId, Type.FLOW);
    Assert.assertEquals(initialInstances + 5,
                        program.getSpecification().
                          getFlows().get("WordCountFlow").getFlowlets().get("StreamSource").getInstances());
  }

  @Test
  public void testProcedureInstances() throws Exception {

    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = getSpec(new AllProgramsApp().configure());

    Id.Application appId = new Id.Application(new Id.Account(DefaultId.ACCOUNT.getId()), spec.getName());
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
    ApplicationSpecification spec = getSpec(new WordCountApp().configure());
    Id.Account accountId = new Id.Account("account1");
    Id.Application appId = new Id.Application(accountId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertEquals(1, store.getAllStreams(new Id.Account("account1")).size());
    Assert.assertEquals(1, store.getAllDataSets(new Id.Account("account1")).size());

    // removing flow
    store.removeAllApplications(accountId);

    Assert.assertNull(store.getApplication(appId));
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, store.getAllStreams(new Id.Account("account1")).size());
    Assert.assertEquals(1, store.getAllDataSets(new Id.Account("account1")).size());
  }

  @Test
  public void testRemoveAll() throws Exception {
    ApplicationSpecification spec = getSpec(new WordCountApp().configure());
    Id.Account accountId = new Id.Account("account1");
    Id.Application appId = new Id.Application(accountId, "application1");
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertEquals(1, store.getAllStreams(new Id.Account("account1")).size());
    Assert.assertEquals(1, store.getAllDataSets(new Id.Account("account1")).size());

    // removing flow
    store.removeAll(accountId);

    Assert.assertNull(store.getApplication(appId));
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, store.getAllStreams(new Id.Account("account1")).size());
    Assert.assertEquals(1, store.getAllDataSets(new Id.Account("account1")).size());
  }

  @Test
  public void testRemoveApplication() throws Exception {
    ApplicationSpecification spec = getSpec(new WordCountApp().configure());
    Id.Account accountId = new Id.Account("account1");
    Id.Application appId = new Id.Application(accountId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertEquals(1, store.getAllStreams(new Id.Account("account1")).size());
    Assert.assertEquals(1, store.getAllDataSets(new Id.Account("account1")).size());

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, store.getAllStreams(new Id.Account("account1")).size());
    Assert.assertEquals(1, store.getAllDataSets(new Id.Account("account1")).size());
  }

  @Test
  public void testRuntimeArgsDeletion() throws Exception {
    ApplicationSpecification spec = getSpec(new AllProgramsApp().configure());
    Id.Account accountId = new Id.Account("testDeleteRuntimeArgs");
    Id.Application appId = new Id.Application(accountId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));

    Id.Program flowProgramId = new Id.Program(appId, "NoOpFlow");
    Id.Program mapreduceProgramId = new Id.Program(appId, "NoOpMR");
    Id.Program procedureProgramId = new Id.Program(appId, "NoOpProcedure");
    Id.Program workflowProgramId = new Id.Program(appId, "NoOpWorkflow");

    store.storeRunArguments(flowProgramId, ImmutableMap.of("model", "click"));
    store.storeRunArguments(mapreduceProgramId, ImmutableMap.of("path", "/data"));
    store.storeRunArguments(procedureProgramId, ImmutableMap.of("timeoutMs", "1000"));
    store.storeRunArguments(workflowProgramId, ImmutableMap.of("whitelist", "continuuity"));


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
    Assert.assertEquals("continuuity", args.get("whitelist"));

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
  public void testCheckDeletedProgramSpecs () throws Exception {
    //Deploy program with all types of programs.
    AppFabricTestHelper.deployApplication(AllProgramsApp.class);
    ApplicationSpecification spec = getSpec(new AllProgramsApp().configure());

    Set<String> specsToBeVerified = Sets.newHashSet();
    specsToBeVerified.addAll(spec.getProcedures().keySet());
    specsToBeVerified.addAll(spec.getMapReduce().keySet());
    specsToBeVerified.addAll(spec.getWorkflows().keySet());
    specsToBeVerified.addAll(spec.getFlows().keySet());

    //Verify if there are 4 program specs in AllProgramsApp
    Assert.assertEquals(4, specsToBeVerified.size());

    Id.Application appId = Id.Application.from(DefaultId.ACCOUNT, "App");
    // Check the diff with the same app - re-deployement scenario where programs are not removed.
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appId,  spec);
    Assert.assertEquals(0, deletedSpecs.size());

    //Get the spec for app that contains no programs.
    spec = getSpec(new NoProgramsApp().configure());

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
    ApplicationSpecification spec = getSpec(new AllProgramsApp().configure());

    Set<String> specsToBeDeleted = Sets.newHashSet();
    specsToBeDeleted.addAll(spec.getWorkflows().keySet());
    specsToBeDeleted.addAll(spec.getProcedures().keySet());

    Assert.assertEquals(2, specsToBeDeleted.size());

    Id.Application appId = Id.Application.from(DefaultId.ACCOUNT, "App");

    //Get the spec for app that contains only flow and mapreduce - removing procedures and workflows.
    spec = getSpec(new FlowMapReduceApp().configure());

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

}
