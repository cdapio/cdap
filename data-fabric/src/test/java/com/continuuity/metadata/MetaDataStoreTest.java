package com.continuuity.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Stream;
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
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
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
}
