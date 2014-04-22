package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data2.transaction.TransactionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class IndexedObjectStoreTest extends DataSetTestBase {

  static IndexedObjectStore<Feed> indexedFeed;
  static IndexedObjectStore<String> strings;

  @BeforeClass
  public static void configure() throws Exception {
    DataSet index = new IndexedObjectStore<Feed>("index", Feed.class);
    DataSet strings = new IndexedObjectStore<String>("strings", String.class);
    setupInstantiator(Lists.newArrayList(index, strings));
  }


  @Test
  public void testLookupByIndex() throws Exception {

    indexedFeed = instantiator.getDataSet("index");
    TransactionContext txContext = newTransaction();

    List<String> categories1 = ImmutableList.of("racing", "tech");
    List<String> categories2 = ImmutableList.of("electronics", "tech");

    Feed feed1 =  new Feed("f1", "http://f1.com", categories1);
    Feed feed2 =  new Feed("apple", "http://apple.com", categories2);

    byte[] key1 = Bytes.toBytes(feed1.getId());
    byte[] key2 = Bytes.toBytes(feed2.getId());

    indexedFeed.write(key1, feed1, getCategories(categories1));
    indexedFeed.write(key2, feed2, getCategories(categories2));

    List<Feed> feedResult1 = indexedFeed.readAllByIndex(Bytes.toBytes("racing"));
    Assert.assertEquals(1, feedResult1.size());

    List<Feed> feedResult2 = indexedFeed.readAllByIndex(Bytes.toBytes("running"));
    Assert.assertEquals(0, feedResult2.size());

    List<Feed> feedResult3 = indexedFeed.readAllByIndex(Bytes.toBytes("tech"));
    Assert.assertEquals(2, feedResult3.size());

    commitTransaction(txContext);
  }

  @Test
  public void testIndexRewrites() throws Exception {

    indexedFeed = instantiator.getDataSet("index");
    TransactionContext txContext = newTransaction();

    List<String> categories1 = ImmutableList.of("big data", "startup");
    List<String> categories2 = ImmutableList.of("hadoop");

    Feed feed1 =  new Feed("c1", "http://continuuity.com", categories1);
    byte[] key1 = Bytes.toBytes(feed1.getId());

    indexedFeed.write(key1, feed1, getCategories(categories1));

    List<Feed> feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("big data"));
    Assert.assertEquals(1, feedResult.size());

    // re-write with new index values
    indexedFeed.write(key1, feed1, getCategories(categories2));

    //Should not return based on old index values
    feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("big data"));
    Assert.assertEquals(0, feedResult.size());

    feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("hadoop"));
    //Should return based on new indexValue
    Assert.assertEquals(1, feedResult.size());

    //Update with no index value. Lookup by any indexValue should not return the old entries
    indexedFeed.write(key1, feed1);
    feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("hadoop"));
    Assert.assertEquals(0, feedResult.size());

    commitTransaction(txContext);
  }

  @Test
  public void testIndexPruning() throws Exception {

    indexedFeed = instantiator.getDataSet("index");
    TransactionContext txContext = newTransaction();

    List<String> categories = ImmutableList.of("running", "marathon", "drinking");
    Feed feed =  new Feed("rocknroll", "http://rock'n'roll.com", categories);

    byte[] key = Bytes.toBytes(feed.getId());
    indexedFeed.write(key, feed, getCategories(categories));

    List<Feed> feeds = indexedFeed.readAllByIndex(Bytes.toBytes("drinking"));
    Assert.assertEquals(1, feeds.size());

    indexedFeed.pruneIndex(key, Bytes.toBytes("drinking"));
    feeds = indexedFeed.readAllByIndex(Bytes.toBytes("drinking"));
    Assert.assertEquals(0, feeds.size());

    commitTransaction(txContext);
  }

  @Test
  public void testIndexNoSecondaryKeyChanges() throws Exception {
    indexedFeed = instantiator.getDataSet("index");
    List<String> categories = ImmutableList.of("C++", "C#");

    Feed feed1 =  new Feed("MSFT", "http://microwsoft.com", categories);
    byte[] key1 = Bytes.toBytes(feed1.getId());

    indexedFeed.write(key1, feed1, getCategories(categories));

    List<Feed> feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("C++"));
    Assert.assertEquals(1, feedResult.size());

    // re-write f1 with updated url but same id
    Feed feed2 = new Feed("MSFT", "http://microsoft.com", categories);
    indexedFeed.write(key1, feed2, getCategories(categories));

    //Should be still be able to look up by secondary indices

    feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("C++"));
    Assert.assertEquals(1, feedResult.size());

    feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("C#"));
    Assert.assertEquals(1, feedResult.size());

    Assert.assertEquals("http://microsoft.com", feedResult.get(0).getUrl());
  }

  @Test
  public void testIndexUpdates() throws Exception {
    indexedFeed = instantiator.getDataSet("index");
    TransactionContext txContext = newTransaction();

    List<String> categories1 = ImmutableList.of("big data");

    Feed feed1 =  new Feed("a1", "http://apple.com", categories1);
    byte[] key1 = Bytes.toBytes(feed1.getId());

    indexedFeed.write(key1, feed1, getCategories(categories1));

    List<Feed> feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("big data"));
    Assert.assertEquals(1, feedResult.size());

    indexedFeed.updateIndex(key1, Bytes.toBytes("startup"));

    feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("startup"));
    Assert.assertEquals(1, feedResult.size());

    commitTransaction(txContext);
  }


  private byte[][] getCategories(List<String> categories) {
    byte[][] byteCategories = new byte[categories.size()][];
    for (int i = 0; i < categories.size(); i++) {
      byteCategories[i] = Bytes.toBytes(categories.get(i));
    }
    return byteCategories;
  }
}
