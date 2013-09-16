/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.ToyApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.AbstractMapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.store.MDSBasedStore;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.Mapreduce;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class MDSBasedStoreTest {
  private MDSBasedStore store;
  private MetadataService.Iface metadataService;

  // we do it in @Before (not in @BeforeClass) to have easy automatic cleanup between tests
  @Before
  public void before() throws OperationException {
    metadataService = TestHelper.getInjector().getInstance(MetadataService.Iface.class);
    store = TestHelper.getInjector().getInstance(MDSBasedStore.class);

    // clean up data
    MetaDataStore mds = TestHelper.getInjector().getInstance(MetaDataStore.class);
    for (String account : mds.listAccounts(new OperationContext(DefaultId.DEFAULT_ACCOUNT_ID))) {
      mds.clear(new OperationContext(account), account, null);
    }
  }

  @Test
  public void testLoadingProgram() throws Exception {
    TestHelper.deployApplication(ToyApp.class);
    Program program = store.loadProgram(Id.Program.from(DefaultId.ACCOUNT.getId(), "ToyApp", "ToyFlow"), Type.FLOW);
    Assert.assertNotNull(program);
  }

  @Test
  public void testLogProgramRunHistory() throws OperationException {
    // record finished flow
    Id.Program programId = Id.Program.from("account1", "application1", "flow1");
    store.setStart(programId, "run1", 20);
    store.setStop(programId, "run1", 29, "FAILED");

    // record another finished flow
    store.setStart(programId, "run2", 10);
    store.setStop(programId, "run2", 19, "SUCCEEDED");

    // record not finished flow
    store.setStart(programId, "run3", 50);

    // record run of different program
    Id.Program programId2 = Id.Program.from("account1", "application1", "flow2");
    store.setStart(programId2, "run4", 100);
    store.setStop(programId2, "run4", 109, "SUCCEEDED");

    // record for different account
    store.setStart(Id.Program.from("account2", "application1", "flow1"), "run3", 60);

    // we should probably be better with "get" method in MDSBasedStore interface to do that, but we don't have one
    List<RunRecord> history = store.getRunHistory(programId);

    // only finished runs should be returned
    Assert.assertEquals(2, history.size());
    // records should be sorted by start time
    RunRecord run = history.get(0);
    Assert.assertEquals(10, run.getStartTs());
    Assert.assertEquals(19, run.getStopTs());
    Assert.assertEquals("SUCCEEDED", run.getEndStatus());

    run = history.get(1);
    Assert.assertEquals(20, run.getStartTs());
    Assert.assertEquals(29, run.getStopTs());
    Assert.assertEquals("FAILED", run.getEndStatus());

    // testing "get all history for account"
    // note: we need to add account's apps info into store
    store.addApplication(Id.Application.from("account1", "application1"),
                         ApplicationSpecification.Builder.with().setName("application1").setDescription("")
                         .noStream().noDataSet()
                         .withFlows().add(new FlowImpl("flow1")).add(new FlowImpl("flow2"))
                         .noProcedure().build(), new LocalLocationFactory().create("/foo"));
    store.addApplication(Id.Application.from("account2", "application1"),
                         ApplicationSpecification.Builder.with().setName("application1").setDescription("")
                         .noStream().noDataSet()
                         .withFlows().add(new FlowImpl("flow1")).add(new FlowImpl("flow2"))
                         .noProcedure().build(), new LocalLocationFactory().create("/foo"));

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
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Application id = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(id, spec, new LocalLocationFactory().create("/foo/path/application1.jar"));

    ApplicationSpecification stored = store.getApplication(id);
    assertWordCountAppSpecAndInMetadataStore(stored);

    Assert.assertEquals("/foo/path/application1.jar", store.getApplicationArchiveLocation(id).toURI().getPath());
  }

  @Test
  public void testUpdateSameApplication() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
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

    store.addApplication(id, new FooApp().configure(), new LocalLocationFactory().create("/foo"));
    // update
    store.addApplication(id, new ChangedFooApp().configure(), new LocalLocationFactory().create("/foo"));

    ApplicationSpecification stored = store.getApplication(id);
    assertChangedFooAppSpecAndInMetadataStore(stored);
  }

  private static class FooApp implements com.continuuity.api.Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
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
        .withBatch()
          .add(new FooMapReduceJob("mrJob1"))
          .add(new FooMapReduceJob("mrJob2"))
        .build();
    }
  }

  private static class ChangedFooApp implements com.continuuity.api.Application {
    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
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
        .withBatch()
          .add(new FooMapReduceJob("mrJob2"))
          .add(new FooMapReduceJob("mrJob3"))
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
      byte[] val = this.counters.read(word.getBytes(Charsets.UTF_8));
    }
  }


  private void assertWordCountAppSpecAndInMetadataStore(ApplicationSpecification stored)
    throws MetadataServiceException, org.apache.thrift.TException {
    // should be enough to make sure it is stored
    Assert.assertEquals(1, stored.getDataSets().size());
    Assert.assertEquals(WordCountApp.WordCountFlow.class.getName(),
                        stored.getFlows().get("WordCountFlow").getClassName());

    // Checking that resources were registered in metadataService (UI still uses this)
    // app
    Account account1 = new Account("account1");
    Application app = metadataService.getApplication(account1, new Application("application1"));
    Assert.assertNotNull(app);
    Assert.assertEquals("WordCountApp", app.getName());

    // flow
    Assert.assertEquals(1, metadataService.getFlows("account1").size());
    Flow flow = metadataService.getFlow("account1", "application1", "WordCountFlow");
    Assert.assertNotNull(flow);
    Assert.assertEquals(1, flow.getDatasets().size());
    Assert.assertEquals(1, flow.getStreams().size());
    Assert.assertEquals("WordCountFlow", flow.getName());

    // procedure
    Assert.assertEquals(1, metadataService.getQueries(account1).size());
    Query query = metadataService.getQuery(account1, new Query("WordFrequency", "application1"));
    Assert.assertNotNull(query);
    // TODO: uncomment when datasets are added to procedureSpec
//    Assert.assertEquals(1, query.getDatasets().size());
    Assert.assertEquals("WordFrequency", query.getName());

    // mapreduce
    Assert.assertEquals(1, metadataService.getMapreduces(account1).size());
    Mapreduce mapreduce = metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", "application1"));
    Assert.assertEquals("VoidMapReduceJob", mapreduce.getName());

    // streams
    Assert.assertEquals(1, metadataService.getStreams(account1).size());
    Stream stream = metadataService.getStream(account1, new Stream("text"));
    Assert.assertNotNull(stream);
    Assert.assertEquals("text", stream.getName());

    // datasets
    Assert.assertEquals(1, metadataService.getDatasets(account1).size());
    Dataset dataset = metadataService.getDataset(account1, new Dataset("mydataset"));
    Assert.assertNotNull(dataset);
    Assert.assertEquals("mydataset", dataset.getName());
    Assert.assertEquals(KeyValueTable.class.getName(), dataset.getType());
  }

  private void assertChangedFooAppSpecAndInMetadataStore(ApplicationSpecification stored)
    throws MetadataServiceException, org.apache.thrift.TException {
    // should be enough to make sure it is stored
    Assert.assertEquals(2, stored.getDataSets().size());
    Assert.assertEquals(FlowImpl.class.getName(),
                        stored.getFlows().get("flow2").getClassName());

    // Checking that resources were registered in metadataService (UI still uses this).
    // app
    Account account1 = new Account("account1");
    Application app = metadataService.getApplication(account1, new Application("application1"));
    Assert.assertNotNull(app);
    Assert.assertEquals("FooApp", app.getName());

    // flow
    Assert.assertEquals(2, metadataService.getFlows("account1").size());
    Flow flow2 = metadataService.getFlow("account1", "application1", "flow2");
    Assert.assertNotNull(flow2);
    Assert.assertEquals(1, flow2.getDatasets().size());
    Assert.assertEquals(1, flow2.getStreams().size());
    Assert.assertEquals("flow2", flow2.getName());
    Flow flow3 = metadataService.getFlow("account1", "application1", "flow3");
    Assert.assertNotNull(flow3);
    Assert.assertEquals(1, flow3.getDatasets().size());
    Assert.assertEquals(1, flow3.getStreams().size());
    Assert.assertEquals("flow3", flow3.getName());

    // procedure
    Assert.assertEquals(2, metadataService.getQueries(account1).size());
    Query query2 = metadataService.getQuery(account1, new Query("procedure2", "application1"));
    Assert.assertEquals("procedure2", query2.getName());
    Query query3 = metadataService.getQuery(account1, new Query("procedure3", "application1"));
    Assert.assertEquals("procedure3", query3.getName());

    // mapreduce
    Assert.assertEquals(2, metadataService.getMapreduces(account1).size());
    Mapreduce mapreduce2 = metadataService.getMapreduce(account1, new Mapreduce("mrJob2", "application1"));
    Assert.assertEquals("mrJob2", mapreduce2.getName());
    Mapreduce mapreduce3 = metadataService.getMapreduce(account1, new Mapreduce("mrJob3", "application1"));
    Assert.assertEquals("mrJob3", mapreduce3.getName());

    // streams: 3 should be left as streams are not deleted with the application
    Assert.assertEquals(3, metadataService.getStreams(account1).size());
    Stream stream1 = metadataService.getStream(account1, new Stream("stream1"));
    Assert.assertEquals("stream1", stream1.getName());
    Stream stream2 = metadataService.getStream(account1, new Stream("stream2"));
    Assert.assertEquals("stream2", stream2.getName());
    Stream stream3 = metadataService.getStream(account1, new Stream("stream3"));
    Assert.assertEquals("stream3", stream3.getName());

    // datasets: 3 should be left as datasets are not deleted with the application
    Assert.assertEquals(3, metadataService.getDatasets(account1).size());
    Dataset dataset1 = metadataService.getDataset(account1, new Dataset("dataset1"));
    Assert.assertEquals("dataset1", dataset1.getName());
    Assert.assertEquals(Table.class.getName(), dataset1.getType());
    Dataset dataset2 = metadataService.getDataset(account1, new Dataset("dataset2"));
    Assert.assertEquals("dataset2", dataset2.getName());
    Assert.assertEquals(KeyValueTable.class.getName(), dataset2.getType());
    Dataset dataset3 = metadataService.getDataset(account1, new Dataset("dataset3"));
    Assert.assertEquals("dataset3", dataset3.getName());
    Assert.assertEquals(IndexedTable.class.getName(), dataset3.getType());
  }

  @Test
  public void testSetFlowletInstances() throws Exception {
    TestHelper.deployApplication(WordCountApp.class);

    ApplicationSpecification spec = new WordCountApp().configure();
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
  public void testRemoveProgram() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Account accountId = new Id.Account("account1");
    Account account1 = new Account("account1");
    Id.Application id = new Id.Application(accountId, spec.getName());
    store.addApplication(id, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertTrue(
      metadataService.getFlow("account1", id.getId(), "WordCountFlow").isExists());
    Assert.assertTrue(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
    Assert.assertTrue(
      metadataService.getQuery(account1, new Query("WordFrequency", id.getId())).isExists());

    // removing flow
    store.remove(new Id.Program(id, "WordCountFlow"));

    ApplicationSpecification updated = store.getApplication(id);

    // checking that flow was removed
    Assert.assertEquals(0, updated.getFlows().size());

    // checking that it was removed from metadatastore too
    Assert.assertFalse(metadataService.getFlow("account1", id.getId(), "WordCountFlow").isExists());

    // removing query
    store.remove(new Id.Program(id, "WordFrequency"));

    updated = store.getApplication(id);

    // checking that query was removed
    Assert.assertEquals(0, updated.getProcedures().size());

    // checking that it was removed from metadatastore too
    Assert.assertFalse(
      metadataService.getQuery(account1, new Query("WordFrequency", id.getId())).isExists());

    // removing mapreduce
    store.remove(new Id.Program(id, "VoidMapReduceJob"));

    updated = store.getApplication(id);

    // checking that mapreduce was removed
    Assert.assertEquals(0, updated.getMapReduces().size());

    // checking that it was removed from metadatastore too
    Assert.assertFalse(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
  }

  @Test
  public void testRemoveAllApplications() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Account accountId = new Id.Account("account1");
    Account account1 = new Account("account1");
    Id.Application appId = new Id.Application(accountId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    ApplicationSpecification appSpec = store.getApplication(appId);

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertTrue(
      metadataService.getFlow("account1", spec.getName(), "WordCountFlow").isExists());
    Assert.assertTrue(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
    Assert.assertTrue(
      metadataService.getQuery(account1, new Query("WordFrequency", spec.getName())).isExists());
    Assert.assertEquals(1, metadataService.getStreams(account1).size());
    Assert.assertEquals(1, metadataService.getDatasets(account1).size());

    // removing flow
    store.removeAllApplications(accountId);

    Assert.assertNull(store.getApplication(appId));
    Assert.assertFalse(
      metadataService.getFlow("account1", spec.getName(), "WordCountFlow").isExists());
    Assert.assertFalse(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
    Assert.assertFalse(
      metadataService.getQuery(account1, new Query("WordFrequency", spec.getName())).isExists());
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, metadataService.getStreams(account1).size());
    Assert.assertEquals(1, metadataService.getDatasets(account1).size());
  }

  @Test
  public void testRemoveAll() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Account accountId = new Id.Account("account1");
    Account account1 = new Account("account1");
    Id.Application appId = new Id.Application(accountId, "application1");
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertTrue(
      metadataService.getFlow("account1", "application1", "WordCountFlow").isExists());
    Assert.assertTrue(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", "application1")).isExists());
    Assert.assertTrue(
      metadataService.getQuery(account1, new Query("WordFrequency", "application1")).isExists());
    Assert.assertEquals(1, metadataService.getStreams(account1).size());
    Assert.assertEquals(1, metadataService.getDatasets(account1).size());

    // removing flow
    store.removeAll(accountId);

    Assert.assertNull(store.getApplication(appId));
    Assert.assertFalse(
      metadataService.getFlow("account1", "application1", "WordCountFlow").isExists());
    Assert.assertFalse(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
    Assert.assertFalse(
      metadataService.getQuery(account1, new Query("WordFrequency", "application1")).isExists());
    // Streams and DataSets should survive deletion
    Assert.assertEquals(0, metadataService.getStreams(account1).size());
    Assert.assertEquals(0, metadataService.getDatasets(account1).size());
  }

  @Test
  public void testRemoveApplication() throws Exception {
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Account accountId = new Id.Account("account1");
    Account account1 = new Account("account1");
    Id.Application appId = new Id.Application(accountId, spec.getName());
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

    ApplicationSpecification appSpec = store.getApplication(appId);

    Assert.assertNotNull(store.getApplication(appId));
    Assert.assertTrue(
      metadataService.getFlow("account1", spec.getName(), "WordCountFlow").isExists());
    Assert.assertTrue(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
    Assert.assertTrue(
      metadataService.getQuery(account1, new Query("WordFrequency", spec.getName())).isExists());
    Assert.assertEquals(1, metadataService.getStreams(account1).size());
    Assert.assertEquals(1, metadataService.getDatasets(account1).size());

    // removing application
    store.removeApplication(appId);

    Assert.assertNull(store.getApplication(appId));
    Assert.assertFalse(
      metadataService.getApplication(account1, new Application(spec.getName())).isExists());
    Assert.assertFalse(
      metadataService.getFlow("account1", spec.getName(), "WordCountFlow").isExists());
    Assert.assertFalse(
      metadataService.getMapreduce(account1, new Mapreduce("VoidMapReduceJob", spec.getName())).isExists());
    Assert.assertFalse(
      metadataService.getQuery(account1, new Query("WordFrequency", spec.getName())).isExists());
    // Streams and DataSets should survive deletion
    Assert.assertEquals(1, metadataService.getStreams(account1).size());
    Assert.assertEquals(1, metadataService.getDatasets(account1).size());
  }
}
