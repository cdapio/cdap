/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Test for {@link co.cask.cdap.api.dataset.lib.IndexedObjectStore}.
 */
public class IndexedObjectStoreTest extends AbstractDatasetTest {

  private static final Id.DatasetInstance index = Id.DatasetInstance.from(NAMESPACE_ID, "index");

  @Before
  public void createDataset() throws Exception {
    createIndexedObjectStoreInstance(index, Feed.class);
  }

  @After
  public void deleteDataset() throws Exception {
    deleteInstance(index);
  }

  @Test
  public void testLookupByIndex() throws Exception {
    final IndexedObjectStore<Feed> indexedFeed = getInstance(index);
    TransactionExecutor txnl = newTransactionExecutor(indexedFeed);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<String> categories1 = ImmutableList.of("racing", "tech");
        List<String> categories2 = ImmutableList.of("electronics", "tech");

        Feed feed1 = new Feed("f1", "http://f1.com", categories1);
        Feed feed2 = new Feed("apple", "http://apple.com", categories2);

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
      }
    });
  }

  @Test
  public void testIndexRewrites() throws Exception {
    final IndexedObjectStore<Feed> indexedFeed = getInstance(index);
    TransactionExecutor txnl = newTransactionExecutor(indexedFeed);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<String> categories1 = ImmutableList.of("big data", "startup");
        List<String> categories2 = ImmutableList.of("hadoop");

        Feed feed1 =  new Feed("c1", "http://abc.com", categories1);
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
      }
    });
  }

  @Test
  public void testIndexPruning() throws Exception {
    final IndexedObjectStore<Feed> indexedFeed = getInstance(index);
    TransactionExecutor txnl = newTransactionExecutor(indexedFeed);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<String> categories = ImmutableList.of("running", "marathon", "drinking");
        Feed feed = new Feed("rocknroll", "http://rock'n'roll.com", categories);

        byte[] key = Bytes.toBytes(feed.getId());
        indexedFeed.write(key, feed, getCategories(categories));

        List<Feed> feeds = indexedFeed.readAllByIndex(Bytes.toBytes("drinking"));
        Assert.assertEquals(1, feeds.size());

        indexedFeed.pruneIndex(key, Bytes.toBytes("drinking"));
        feeds = indexedFeed.readAllByIndex(Bytes.toBytes("drinking"));
        Assert.assertEquals(0, feeds.size());
      }
    });
  }

  @Test
  public void testIndexNoSecondaryKeyChanges() throws Exception {
    IndexedObjectStore<Feed> indexedFeed = getInstance(index);

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
    final IndexedObjectStore<Feed> indexedFeed = getInstance(index);
    TransactionExecutor txnl = newTransactionExecutor(indexedFeed);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<String> categories1 = ImmutableList.of("big data");

        Feed feed1 = new Feed("a1", "http://apple.com", categories1);
        byte[] key1 = Bytes.toBytes(feed1.getId());

        indexedFeed.write(key1, feed1, getCategories(categories1));

        List<Feed> feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("big data"));
        Assert.assertEquals(1, feedResult.size());

        indexedFeed.updateIndex(key1, Bytes.toBytes("startup"));

        feedResult = indexedFeed.readAllByIndex(Bytes.toBytes("startup"));
        Assert.assertEquals(1, feedResult.size());
      }
    });
  }

  protected void createIndexedObjectStoreInstance(Id.DatasetInstance datasetInstanceId, Type type) throws Exception {
    createInstance("indexedObjectStore", datasetInstanceId,
                   ObjectStores.objectStoreProperties(type, DatasetProperties.EMPTY));
  }

  private byte[][] getCategories(List<String> categories) {
    byte[][] byteCategories = new byte[categories.size()][];
    for (int i = 0; i < categories.size(); i++) {
      byteCategories[i] = Bytes.toBytes(categories.get(i));
    }
    return byteCategories;
  }
}
