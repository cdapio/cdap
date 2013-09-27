package com.continuuity.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.metadata.types.Application;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Flow;
import com.continuuity.metadata.types.Mapreduce;
import com.continuuity.metadata.types.Procedure;
import com.continuuity.metadata.types.Stream;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

/**
 * Tests metadata service functionality.
 */
public class MetaDataStoreTest {

  /** Instance of metadata service. */
  private static MetaDataStore mds;

  /** Instance of account used for tests. */
  private static String account;
  private static Injector injector;

  @BeforeClass
  public static void beforeMetadataService() throws Exception {
    injector = Guice.createInjector(
      new DataFabricModules().getInMemoryModules()
    );
    /* Instance of operation executor */
    injector.getInstance(InMemoryTransactionManager.class).init();
    mds = injector.getInstance(MetaDataStore.class);
    account = "demo";
  }

  @AfterClass
  public static void afterMetadataService() throws Exception {
    // nothing to be done here.
  }

  @Before
  public void cleanMetaData() throws OperationException {
    injector.getInstance(MetaDataTable.class).clear(
      new OperationContext(Constants.DEVELOPER_ACCOUNT_ID), "demo", null);
  }

  /**
   * Tests creation of streams with only Id. This should
   * throw MetadataServiceException.
   */
  @Test(expected = NullPointerException.class)
  public void testCreateStreamWithOnlyId() throws Exception {
    mds.createStream(account, new Stream("id1"));
  }

  /**
   * Tests creation of streams with Id as empty string. This should
   * throw MetadataServiceException.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testCreateStreamWithEmptyId() throws Exception {
    mds.createStream(account, new Stream(""));
  }

  /**
   * Tests creation of stream with only Id and Name. This should
   * throw MetadataServiceException.
   * @throws Exception
   */
  @Test(expected = IllegalArgumentException.class)
  public void testCreateStreamWithIdAndEmptyName() throws Exception {
    Stream stream = new Stream("id1");
    stream.setName("");
    mds.createStream(account, stream);
  }

  /**
   * Tests creation of stream with all the necessary information.
   * This test should not throw any errors.
   * @throws Exception
   */
  @Test
  public void testCreateStreamCorrect() throws Exception {
    Stream stream = new Stream("id1");
    stream.setName("Funny stream");
    stream.setDescription("Funny stream that is so funny. You laugh it out");
    Assert.assertTrue(mds.createStream(account, stream));
    // Check if there is 1 stream available. Don't need to worry about
    // what's in there. We will do that later.
    Assert.assertTrue(mds.getStreams(account).size() > 0);
  }

  /**
   * Adds a stream "id2" and deletes it.
   * @throws Exception
   */
  @Test
  public void testDeleteStream() throws Exception {
    int count = mds.getStreams(account).size();

    Stream stream = new Stream("id2");
    stream.setName("Serious stream");
    stream.setDescription("Serious stream. Shutup");
    Assert.assertTrue(mds.createStream(account, stream));

    int afterAddCount = mds.getStreams(account).size();
    // Delete the stream now.
    Assert.assertTrue(mds.deleteStream(account, stream.getId()));
    int afterDeleteCount = mds.getStreams(account).size();
    Assert.assertTrue(count == afterAddCount - 1);
    Assert.assertTrue((afterAddCount - 1) == afterDeleteCount);
  }

  /**
   * Tests listing of streams for a given account.
   * @throws Exception
   */
  @Test
  public void testListStream() throws Exception {
    int before = mds.getStreams(account).size();
    Stream stream = new Stream("id3");
    stream.setName("Serious stream");
    stream.setDescription("Serious stream. Shutup");
    Assert.assertTrue(mds.createStream(account, stream));
    Collection<Stream> streams
      = mds.getStreams(account);
    int after = streams.size();
    Assert.assertTrue(after == before + 1);
    for (Stream s : streams) {
      if (s.getId().equals("id3")) {
        Assert.assertTrue("Serious stream".equals(s.getName()));
        Assert.assertTrue("Serious stream. Shutup".equals(s.getDescription()));
      }
    }
    Assert.assertTrue(mds.getStreams("abc").isEmpty());
  }

  public void testCreateDataset() throws Exception {
    Dataset dataset = new Dataset("dataset1");
    dataset.setName("Data Set1");
    dataset.setType("counter");
    dataset.setDescription("test dataset");
    Assert.assertTrue(mds.createDataset(account, dataset));
    List<Dataset> dlist = mds.getDatasets(account);
    Assert.assertNotNull(dlist);
    Assert.assertTrue(dlist.size() > 0);
  }

  @Test
  public void testCreateDeleteListDataSet() throws Exception {
    testCreateDataset(); // creates a dataset.
    Assert.assertNotNull(mds.getDataset(account, "dataset1"));
    // Now delete it.
    Assert.assertTrue(mds.deleteDataset(account, "dataset1"));
    Assert.assertNull(mds.getDataset(account, "dataset1"));
    Assert.assertTrue(mds.getDatasets(account).isEmpty());
  }

  public void testCreateQuery() throws Exception {
    Procedure procedure = new Procedure("query1", "appX");
    procedure.setName("Query 1");
    procedure.setServiceName("myname");
    procedure.setDescription("test dataset");
    Assert.assertTrue(mds.createProcedure(account, procedure));
    List<Procedure> dlist = mds.getProcedures(account);
    Assert.assertNotNull(dlist);
    Assert.assertTrue(dlist.size() > 0);
  }

  @Test
  public void testCreateDeleteListQuery() throws Exception {
    testCreateQuery(); // creates a dataset.
    // Now delete it.
    Assert.assertTrue(mds.deleteProcedure(account, "appX", "query1"));
    List<Procedure> qlist = mds.getProcedures(account);
    Assert.assertTrue(qlist.isEmpty());
    Assert.assertNull(mds.getProcedure(account, "appX", "query1"));
  }

  @Test
  public void testCreateMapreduce() throws Exception {
    Mapreduce mapreduce = new Mapreduce("mr1", "appX");
    mapreduce.setName("Mapreduce 1");
    mapreduce.setDescription("test dataset");
    Assert.assertTrue(mds.createMapreduce(account, mapreduce));
    List<Mapreduce> dlist = mds.getMapreduces(account);
    Assert.assertNotNull(dlist);
    Assert.assertTrue(dlist.size() > 0);
  }

  @Test
  public void testCreateDeleteListMapreduce() throws Exception {
    testCreateMapreduce(); // creates a dataset.
    Assert.assertNotNull(mds.getMapreduce(account, "appX", "mr1"));
    // Now delete it.
    Assert.assertTrue(mds.deleteMapreduce(account, "appX", "mr1"));
    Assert.assertNull(mds.getMapreduce(account, "appX", "mr1"));
    Assert.assertTrue(mds.getMapreduces(account).isEmpty());
  }

  /**
   * Tests creation of a application.
   * @throws Exception
   */
  @Test
  public void testCreateApplication() throws Exception {
    Application application = new Application("app1");
    application.setName("Application 1");
    application.setDescription("Test application");
    Assert.assertNull(mds.getApplication(account, application.getId()));
    Assert.assertTrue(mds.createApplication(account, application));
    Assert.assertNotNull(mds.getApplication(account, application.getId()));
  }

  /**
   * Tests update of a application.
   * @throws Exception
   */
  @Test
  public void testUpdateApplication() throws Exception {
    Application application = new Application("app1");
    application.setName("Application 1");
    application.setDescription("Test application");

    // Create application for first time.
    Assert.assertTrue(mds.createApplication(account, application));
    Assert.assertTrue(mds.getApplications(account).size() > 0);

    // We check what's in there.
    String beforeUpdateDescription = mds.getApplication(account, application.getId()).getDescription();
    Assert.assertTrue("Test application".equals(beforeUpdateDescription));

    // Now, we change the application description.
    Application updateApplication = new Application("app1");
    updateApplication.setDescription("Test updating application");
    updateApplication.setName("Application 1");
    Assert.assertTrue(mds.updateApplication(account, updateApplication));
    String afterUpdateDescription = mds.getApplication(account, application.getId()).getDescription();

    // We validate that it's updated.
    Assert.assertTrue("Test updating application".equals(afterUpdateDescription));
  }


  /**
   * Tests deletion of a application.
   * @throws Exception
   */
  @Test
  public void testDeleteApplication() throws Exception {
    int beforeAddCount = mds.getApplications(account).size();
    Application application = new Application("delapp1");
    application.setName("Application 1");
    application.setDescription("Test application");
    Assert.assertTrue(mds.createApplication(account, application));
    Assert.assertTrue(mds.getApplications(account).size() > 0);
    int afterAddCount = mds.getApplications(account).size();
    Assert.assertTrue(mds.deleteApplication(account, application.getId()));
    int afterDeleteCount = mds.getApplications(account).size();
    Assert.assertTrue((beforeAddCount + 1) == afterAddCount);
    Assert.assertTrue((afterAddCount - 1) == afterDeleteCount);
  }

  /**
   * Tests listing of applications.
   * @throws Exception
   */
  @Test
  public void testListApplication() throws Exception {
    int before = mds.getApplications(account).size();
    Application application = new Application("tapp1");
    application.setName("Serious App");
    application.setDescription("Serious App. Shutup");
    Assert.assertTrue(mds.createApplication(account, application));
    Collection<Application> applications = mds.getApplications(account);
    int after = applications.size();
    Assert.assertTrue(after == before + 1);
    for (Application a : applications) {
      if (a.getId().equals("tapp1")) {
        Assert.assertTrue("Serious App".equals(a.getName()));
        Assert.assertTrue("Serious App. Shutup".equals(a.getDescription()));
      }
    }
    Assert.assertTrue(mds.getApplications("abc").isEmpty());
  }

  /**
   * Tests listing of applications.
   * @throws Exception
   */
  @Test
  public void testFlowAndQueryStuff() throws Exception {

    // clean up streams/queries in mds if there are leftovers from other tests
    for (Stream stream : mds.getStreams(account)) {
      Assert.assertTrue(mds.deleteStream(account, stream.getId()));
    }
    for (Procedure procedure : mds.getProcedures(account)) {
      Assert.assertTrue(mds.deleteProcedure(account, procedure.getApplication(), procedure.getId()));
    }

    List<String> listAB = Lists.newArrayList(), listAC = Lists.newArrayList(),
        listA = Lists.newArrayList(), listAD = Lists.newArrayList();
    listAB.add("a"); listAB.add("b");
    listAC.add("a"); listAC.add("c");
    listA.add("a");
    listAD.add("a"); listAD.add("d");

    Stream streamA = new Stream("a"); streamA.setName("stream A");
    streamA.setDescription("an a");
    Stream streamB = new Stream("b"); streamB.setName("stream B");
    streamB.setDescription("a b");
    Stream streamC = new Stream("c"); streamC.setName("stream C");
    streamC.setDescription("a c");

    Assert.assertTrue(mds.createStream(account, streamA));
    Assert.assertTrue(mds.createStream(account, streamB));
    Assert.assertTrue(mds.createStream(account, streamC));

    Dataset datasetA = new Dataset("a"); datasetA.setName("dataset A");
    datasetA.setDescription("an a"); datasetA.setType("typeA");
    Dataset datasetB = new Dataset("b"); datasetB.setName("dataset B");
    datasetB.setDescription("a b"); datasetB.setType("typeB");
    Dataset datasetC = new Dataset("c"); datasetC.setName("dataset C");
    datasetC.setDescription("a c"); datasetC.setType("typeC");
    Dataset datasetD = new Dataset("d"); datasetD.setName("dataset D");
    datasetD.setDescription("a d"); datasetD.setType("typeD");

    Assert.assertTrue(mds.createDataset(account, datasetA));
    Assert.assertTrue(mds.createDataset(account, datasetB));
    Assert.assertTrue(mds.createDataset(account, datasetC));
    Assert.assertTrue(mds.createDataset(account, datasetD));

    Flow flow1 = new Flow("f1", "app1"); flow1.setName("flow 1");
    flow1.setStreams(listAB); flow1.setDatasets(listAB);
    Flow flow2 = new Flow("f2", "app2"); flow2.setName("flow 2");
    flow2.setStreams(listAC); flow2.setDatasets(listAC);
    Flow flow3 = new Flow("f1", "app2"); flow3.setName("flow 1");
    flow3.setStreams(listAB); flow3.setDatasets(listAB);

    // add flow1, verify get and list
    Assert.assertTrue(mds.createFlow(account, flow1));
    Assert.assertEquals(flow1, mds.getFlow(account, "app1", "f1"));
    List<Flow> flows = mds.getFlows(account);
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    flows = mds.getFlowsByApplication(account, "app1");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow1));

    // add flow2 and flow3, verify get and list
    Assert.assertTrue(mds.createFlow(account, flow2));
    Assert.assertEquals(flow2, mds.getFlow(account, "app2", "f2"));
    Assert.assertTrue(mds.createFlow(account, flow3));
    Assert.assertEquals(flow3, mds.getFlow(account, "app2", "f1"));
    flows = mds.getFlows(account);
    Assert.assertEquals(3, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    Assert.assertTrue(flows.contains(flow2));
    Assert.assertTrue(flows.contains(flow3));
    flows = mds.getFlowsByApplication(account, "app1");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    flows = mds.getFlowsByApplication(account, "app2");
    Assert.assertEquals(2, flows.size());
    Assert.assertTrue(flows.contains(flow2));
    Assert.assertTrue(flows.contains(flow3));

    // list the flows for stream B and C and verify
    flows = mds.getFlowsByStream(account, "b");
    Assert.assertEquals(2, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    Assert.assertTrue(flows.contains(flow3));
    flows = mds.getFlowsByStream(account, "c");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow2));

    // list the flows for dataset B and C and verify
    flows = mds.getFlowsByDataset(account, "b");
    Assert.assertEquals(2, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    Assert.assertTrue(flows.contains(flow3));
    flows = mds.getFlowsByDataset(account, "c");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow2));

    // list and verify streams for account, app1 and app2
    List<Stream> streams = mds.getStreams(account);
    Assert.assertEquals(3, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    Assert.assertTrue(streams.contains(streamC));
    streams = mds.getStreamsByApplication(account, "app1");
    Assert.assertEquals(2, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    streams = mds.getStreamsByApplication(account, "app2");
    Assert.assertEquals(3, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    Assert.assertTrue(streams.contains(streamC));

    Procedure proc1 = new Procedure("q1", "app1"); proc1.setName("q1");
    proc1.setServiceName("q1"); proc1.setDatasets(listAB);
    Procedure proc2 = new Procedure("q2", "app2"); proc2.setName("q2");
    proc2.setServiceName("q2"); proc2.setDatasets(listAC);
    Procedure proc3 = new Procedure("q1", "app2"); proc3.setName("q1");
    proc3.setServiceName("q1"); proc3.setDatasets(listAD);

    // add query1, verify get and list
    Assert.assertTrue(mds.createProcedure(account, proc1));
    Assert.assertEquals(proc1, mds.getProcedure(account, "app1", "q1"));
    List<Procedure> proceduress = mds.getProcedures(account);
    Assert.assertEquals(1, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc1));
    proceduress = mds.getProceduresByApplication(account, "app1");
    Assert.assertEquals(1, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc1));

    // add query2 and query3, verify get and list
    Assert.assertTrue(mds.createProcedure(account, proc2));
    Assert.assertEquals(proc2, mds.getProcedure(account, "app2", "q2"));
    Assert.assertTrue(mds.createProcedure(account, proc3));
    Assert.assertEquals(proc3, mds.getProcedure(account, "app2", "q1"));
    proceduress = mds.getProcedures(account);
    Assert.assertEquals(3, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc1));
    Assert.assertTrue(proceduress.contains(proc2));
    Assert.assertTrue(proceduress.contains(proc3));
    proceduress = mds.getProceduresByApplication(account, "app1");
    Assert.assertEquals(1, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc1));
    proceduress = mds.getProceduresByApplication(account, "app2");
    Assert.assertEquals(2, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc2));
    Assert.assertTrue(proceduress.contains(proc3));

    // list and verify datasets for account, app1 and app2
    List<Dataset> datasets = mds.getDatasets(account);
    Assert.assertEquals(4, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    Assert.assertTrue(datasets.contains(datasetC));
    Assert.assertTrue(datasets.contains(datasetD));
    datasets = mds.getDatasetsByApplication(account, "app1");
    Assert.assertEquals(2, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    streams = mds.getStreamsByApplication(account, "app2");
    Assert.assertEquals(3, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    Assert.assertTrue(streams.contains(streamC));
    datasets = mds.getDatasetsByApplication(account, "app2");
    Assert.assertEquals(4, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    Assert.assertTrue(datasets.contains(datasetC));
    Assert.assertTrue(datasets.contains(datasetD));

    // list the queries for dataset B and C and verify
    proceduress = mds.getQueriesByDataset(account, "a");
    Assert.assertEquals(3, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc1));
    Assert.assertTrue(proceduress.contains(proc2));
    Assert.assertTrue(proceduress.contains(proc3));
    proceduress = mds.getQueriesByDataset(account, "c");
    Assert.assertEquals(1, proceduress.size());
    Assert.assertTrue(proceduress.contains(proc2));

    // delete query3, list again and verify (D should be gone now)
    Assert.assertTrue(mds.deleteProcedure(account, proc3.getApplication(), proc3.getId()));
    datasets = mds.getDatasetsByApplication(account, "app2");
    Assert.assertEquals(3, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    Assert.assertTrue(datasets.contains(datasetC));
    Assert.assertFalse(datasets.contains(datasetD));

    // update flow3 to have listA, list and verify streams and datasets again
    flow3.setStreams(listA);
    flow3.setDatasets(listA);
    Assert.assertTrue(mds.updateFlow(account, flow3));
    Assert.assertEquals(flow3, mds.getFlow(account, "app2", "f1"));
    streams = mds.getStreamsByApplication(account, "app2");
    Assert.assertEquals(2, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamC));
    datasets = mds.getDatasetsByApplication(account, "app2");
    Assert.assertEquals(2, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetC));

    // delete flow2 and query2 verify flows, streams and datasets for app2
    Assert.assertTrue(mds.deleteFlow(account, "app2", "f2"));
    Assert.assertTrue(mds.deleteProcedure(account, proc2.getApplication(), proc2.getId()));
    Assert.assertNull(mds.getFlow(account, "app2", "f2"));
    flows = mds.getFlowsByApplication(account, "app2");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow3));
    streams = mds.getStreamsByApplication(account, "app2");
    Assert.assertEquals(1, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    datasets = mds.getDatasetsByApplication(account, "app2");
    Assert.assertEquals(1, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));

    // add streamB and datasetB back to flow3 using addToFlow
    Assert.assertTrue(mds.addStreamToFlow(account, "app2", "f1", "b"));
    Assert.assertTrue(mds.addDatasetToFlow(account, "app2", "f1", "b"));
    Assert.assertTrue(mds.addDatasetToFlow(account, "app2", "f1", "b"));

    // now verify the streams and datasets for app2 again
    streams = mds.getStreamsByApplication(account, "app2");
    Assert.assertEquals(2, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    datasets = mds.getDatasetsByApplication(account, "app2");
    Assert.assertEquals(2, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));

    // add datasetC to query1 using addToQuery
    Assert.assertTrue(mds.addDatasetToProcedure(account, "app1", "q1", "c"));

    // now verify the datasets for app1 again
    datasets = mds.getDatasetsByApplication(account, "app1");
    Assert.assertEquals(3, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    Assert.assertTrue(datasets.contains(datasetC));

    // wipe out everything
    mds.deleteAll(account);
    // verify that all apps, flows, queries, datasets and streams are gone
    Assert.assertEquals(0, mds.getApplications(account).size());
    Assert.assertEquals(0, mds.getFlows(account).size());
    Assert.assertEquals(0, mds.getProcedures(account).size());
    Assert.assertEquals(0, mds.getDatasets(account).size());
    Assert.assertEquals(0, mds.getStreams(account).size());

  }


}
