package com.continuuity.metadata;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metadata.thrift.*;
import com.continuuity.runtime.MetadataModules;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Tests metadata service functionality.
 */
public class MetadataServiceTest {
  /** Instance of operation executor */
  private static OperationExecutor opex;

  /** Instance of metadata service. */
  private static MetadataService mds;

  /** Instance of account used for tests. */
  private static Account account;

  @BeforeClass
  public static void beforeMetadataService() throws Exception {
    Injector injector = Guice.createInjector(
      new MetadataModules().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules()
    );
    opex = injector.getInstance(OperationExecutor.class);
    mds = new MetadataService(opex);
    account = new Account("demo");
  }

  @AfterClass
  public static void afterMetadataService() throws Exception {
    // nothing to be done here.
  }

  /**
   * Tests creation of streams with only Id. This should
   * throw MetadataServiceException.
   */
  @Test(expected = MetadataServiceException.class)
  public void testCreateStreamWithOnlyId() throws Exception {
    com.continuuity.metadata.thrift.Stream
        stream = new com.continuuity.metadata.thrift.Stream("id1");
    mds.createStream(account, stream);
    Assert.assertTrue(true);
  }

  /**
   * Tests creation of streams with Id as empty string. This should
   * throw MetadataServiceException.
   */
  @Test(expected = MetadataServiceException.class)
  public void testCreateStreamWithEmptyId() throws Exception {
    com.continuuity.metadata.thrift.Stream
      stream = new com.continuuity.metadata.thrift.Stream("");
    mds.createStream(account, stream);
    Assert.assertTrue(true);
  }

  /**
   * Tests creation of stream with only Id and Name. This should
   * throw MetadataServiceException.
   * @throws Exception
   */
  @Test
  public void testCreateStreamWithIdAndName() throws Exception {
    Stream
      stream = new Stream("id1");
    stream.setName("Funny stream");
    mds.createStream(account, stream);
    Assert.assertTrue(true);
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
    Assert.assertTrue(mds.deleteStream(account, stream));
    int afterDeleteCount = mds.getStreams(account).size();
    Assert.assertTrue(count == afterAddCount-1);
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
    for(Stream s : streams) {
      if(s.getId().equals("id3")) {
        Assert.assertTrue("Serious stream".equals(s.getName()));
        Assert.assertTrue("Serious stream. Shutup".equals(s.getDescription()));
      }
    }
    Account account1 = new Account("abc");
    Assert.assertTrue(mds.getStreams(account1).size() == 0);
  }

  @Test
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
    // Now delete it.
    Dataset dataset = new Dataset("dataset1");
    Assert.assertNotNull(mds.deleteDataset(account, dataset));
    List<Dataset> dlist = mds.getDatasets(account);
    Assert.assertTrue(dlist.size() == 0);
    Dataset dataset1 = mds.getDataset(account, dataset);
    Assert.assertNotNull(dataset1);
  }

  @Test
  public void testCreateQuery() throws Exception {
    Query query = new Query("query1", "app1");
    query.setName("Query 1");
    query.setServiceName("myname");
    query.setDescription("test dataset");
    Assert.assertTrue(mds.createQuery(account, query));
    List<Query> dlist = mds.getQueries(account);
    Assert.assertNotNull(dlist);
    Assert.assertTrue(dlist.size() > 0);
  }

  @Test
  public void testCreateDeleteListQuery() throws Exception {
    testCreateQuery(); // creates a dataset.
    // Now delete it.
    Query query = new Query("query1", "app1");
    Assert.assertNotNull(mds.deleteQuery(account, query));
    List<Query> qlist = mds.getQueries(account);
    Assert.assertTrue(qlist.size() == 0);
    Query query1 = mds.getQuery(account, query);
    Assert.assertNotNull(query1);
  }

  /**
   * Tests creation of a stream.
   * @throws Exception
   */
  @Test
  public void testCreateApplication() throws Exception {
    Application application = new Application("app1");
    application.setName("Application 1");
    application.setDescription("Test application");
    Assert.assertTrue(mds.createApplication(account, application));
    Assert.assertTrue(mds.getApplications(account).size() > 0);
  }

  /**
   * Tests deletion of a stream.
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
    Application applicationToDelete = new Application("delapp1");
    Assert.assertTrue(mds.deleteApplication(account, applicationToDelete));
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
    for(Application a : applications) {
      if(a.getId().equals("tapp1")) {
        Assert.assertTrue("Serious App".equals(a.getName()));
        Assert.assertTrue("Serious App. Shutup".equals(a.getDescription()));
      }
    }
    Account account1 = new Account("abc");
    Assert.assertTrue(mds.getApplications(account1).size() == 0);
  }

  /**
   * Tests listing of applications.
   * @throws Exception
   */
  @Test
  public void testFlowStuff() throws Exception {

    // clean up streams in mds if there are any leftover from other tests
    for (Stream stream : mds.getStreams(account)) {
      Assert.assertTrue(mds.deleteStream(account, stream));
    }

    List<String> listAB = Lists.newArrayList(), listAC = Lists.newArrayList(),
        mtList = Collections.emptyList(), listA = Lists.newArrayList();
    listAB.add("a"); listAB.add("b");
    listAC.add("a"); listAC.add("c");
    listA.add("a");

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

    Assert.assertTrue(mds.createDataset(account, datasetA));
    Assert.assertTrue(mds.createDataset(account, datasetB));
    Assert.assertTrue(mds.createDataset(account, datasetC));

    Flow flow1 = new Flow("f1", "app1", "flow 1", listAB, listAB);
    Flow flow2 = new Flow("f2", "app2", "flow 2", listAC, listAC);
    Flow flow3 = new Flow("f1", "app2", "flow 1", listAB, listAB);

    // add flow1, verify get and list
    Assert.assertTrue(mds.createFlow(account.getId(), flow1));
    Assert.assertEquals(flow1, mds.getFlow(account.getId(), "app1", "f1"));
    List<Flow> flows = mds.getFlows(account.getId());
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    flows = mds.getFlowsByApplication(account.getId(), "app1");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow1));

    // add flow2 and flow3, verify get and list
    Assert.assertTrue(mds.createFlow(account.getId(), flow2));
    Assert.assertEquals(flow2, mds.getFlow(account.getId(), "app2", "f2"));
    Assert.assertTrue(mds.createFlow(account.getId(), flow3));
    Assert.assertEquals(flow3, mds.getFlow(account.getId(), "app2", "f1"));
    flows = mds.getFlows(account.getId());
    Assert.assertEquals(3, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    Assert.assertTrue(flows.contains(flow2));
    Assert.assertTrue(flows.contains(flow3));
    flows = mds.getFlowsByApplication(account.getId(), "app1");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    flows = mds.getFlowsByApplication(account.getId(), "app2");
    Assert.assertEquals(2, flows.size());
    Assert.assertTrue(flows.contains(flow2));
    Assert.assertTrue(flows.contains(flow3));

    // list the flows for stream B and C and verify
    flows = mds.getFlowsByStream(account.getId(), "b");
    Assert.assertEquals(2, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    Assert.assertTrue(flows.contains(flow3));
    flows = mds.getFlowsByStream(account.getId(), "c");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow2));

    // list the flows for dataset B and C and verify
    flows = mds.getFlowsByDataset(account.getId(), "b");
    Assert.assertEquals(2, flows.size());
    Assert.assertTrue(flows.contains(flow1));
    Assert.assertTrue(flows.contains(flow3));
    flows = mds.getFlowsByDataset(account.getId(), "c");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow2));

    // list and verify streams for account, app1 and app2
    List<Stream> streams = mds.getStreams(account);
    Assert.assertEquals(3, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    Assert.assertTrue(streams.contains(streamC));
    streams = mds.getStreamsByApplication(account.getId(), "app1");
    Assert.assertEquals(2, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    streams = mds.getStreamsByApplication(account.getId(), "app2");
    Assert.assertEquals(3, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    Assert.assertTrue(streams.contains(streamC));

    // list and verify datasets for account, app1 and app2
    List<Dataset> datasets = mds.getDatasets(account);
    Assert.assertEquals(3, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    Assert.assertTrue(datasets.contains(datasetC));
    datasets = mds.getDatasetsByApplication(account.getId(), "app1");
    Assert.assertEquals(2, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    streams = mds.getStreamsByApplication(account.getId(), "app2");
    Assert.assertEquals(3, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    Assert.assertTrue(streams.contains(streamC));
    datasets = mds.getDatasetsByApplication(account.getId(), "app2");
    Assert.assertEquals(3, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
    Assert.assertTrue(datasets.contains(datasetC));

    // update flow3 to have listA, list and verify streams and datasets again
    flow3.setStreams(listA);
    flow3.setDatasets(listA);
    Assert.assertTrue(mds.updateFlow(account.getId(), flow3));
    Assert.assertEquals(flow3, mds.getFlow(account.getId(), "app2", "f1"));
    streams = mds.getStreamsByApplication(account.getId(), "app2");
    Assert.assertEquals(2, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamC));
    datasets = mds.getDatasetsByApplication(account.getId(), "app2");
    Assert.assertEquals(2, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetC));

    // delete flow2, verify flows, streams and datasets for app2
    Assert.assertTrue(mds.deleteFlow(account.getId(), "app2", "f2"));
    Assert.assertFalse(mds.getFlow(account.getId(), "app2", "f2").isExists());
    flows = mds.getFlowsByApplication(account.getId(), "app2");
    Assert.assertEquals(1, flows.size());
    Assert.assertTrue(flows.contains(flow3));
    streams = mds.getStreamsByApplication(account.getId(), "app2");
    Assert.assertEquals(1, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    datasets = mds.getDatasetsByApplication(account.getId(), "app2");
    Assert.assertEquals(1, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));

    // add streamB and datasetB back to flow3 using addToFlow
    Assert.assertTrue(mds.addStreamToFlow(account.getId(), "app2", "f1", "b"));
    Assert.assertTrue(mds.addDatasetToFlow(account.getId(), "app2", "f1", "b"));

    // now verify the streams and datasets for app2 again
    streams = mds.getStreamsByApplication(account.getId(), "app2");
    Assert.assertEquals(2, streams.size());
    Assert.assertTrue(streams.contains(streamA));
    Assert.assertTrue(streams.contains(streamB));
    datasets = mds.getDatasetsByApplication(account.getId(), "app2");
    Assert.assertEquals(2, datasets.size());
    Assert.assertTrue(datasets.contains(datasetA));
    Assert.assertTrue(datasets.contains(datasetB));
  }


}
