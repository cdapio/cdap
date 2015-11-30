/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.cache;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

public abstract class DynamicDatasetCacheTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  protected static final Id.Namespace NAMESPACE = DatasetFrameworkTestUtil.NAMESPACE_ID;
  protected static DatasetFramework dsFramework;
  protected static TransactionSystemClient txClient;
  protected DynamicDatasetCache cache;

  @BeforeClass
  public static void init() throws DatasetManagementException, IOException {
    dsFramework = dsFrameworkUtil.getFramework();
    dsFramework.addModule(Id.DatasetModule.from(NAMESPACE, "testDataset"), new TestDatasetModule());
    txClient = new InMemoryTxSystemClient(dsFrameworkUtil.getTxManager());
    dsFrameworkUtil.createInstance("testDataset", Id.DatasetInstance.from(NAMESPACE, "a"), DatasetProperties.EMPTY);
    dsFrameworkUtil.createInstance("testDataset", Id.DatasetInstance.from(NAMESPACE, "b"), DatasetProperties.EMPTY);
    dsFrameworkUtil.createInstance("testDataset", Id.DatasetInstance.from(NAMESPACE, "c"), DatasetProperties.EMPTY);
  }

  @AfterClass
  public static void tearDown() throws IOException, DatasetManagementException {
    dsFrameworkUtil.deleteInstance(Id.DatasetInstance.from(NAMESPACE, "a"));
    dsFrameworkUtil.deleteInstance(Id.DatasetInstance.from(NAMESPACE, "b"));
    dsFrameworkUtil.deleteModule(Id.DatasetModule.from(NAMESPACE, "testDataset"));
  }

  @Before
  public void initCache() {
    SystemDatasetInstantiator instantiator =
      new SystemDatasetInstantiator(dsFramework, getClass().getClassLoader(), null);
    Map<String, String> arguments = ImmutableMap.of("key", "k", "value", "v");
    Map<String, String> bArguments = ImmutableMap.of("value", "b");
    Map<String, Map<String, String>> staticDatasets =
      ImmutableMap.of("a", DatasetDefinition.NO_ARGUMENTS, "b", bArguments);
    cache = createCache(instantiator, arguments, staticDatasets);
  }

  protected abstract DynamicDatasetCache createCache(SystemDatasetInstantiator instantiator,
                                                     Map<String, String> arguments,
                                                     Map<String, Map<String, String>> staticDatasets);

  @After
  public void closeCache() {
    if (cache != null) {
      cache.close();
    }
  }

  /**
   * @param datasetMap if not null, this test method will save the instances of a and b into the map.
   */
  protected void testDatasetCache(@Nullable Map<String, TestDataset> datasetMap)
    throws IOException, DatasetManagementException, TransactionFailureException {

    // test that the static datasets are always the same
    TestDataset a = cache.getDataset("a");
    TestDataset a1 = cache.getDataset("a");
    TestDataset a2 = cache.getDataset("a", DatasetDefinition.NO_ARGUMENTS);
    Assert.assertSame(a, a1);
    Assert.assertSame(a, a2);

    TestDataset b = cache.getDataset("b", ImmutableMap.of("value", "b"));
    TestDataset b1 = cache.getDataset("b", ImmutableMap.of("value", "b"));
    Assert.assertSame(b, b1);

    // validate that arguments for a are the global runtime args of the cache
    Assert.assertEquals(2, a.getArguments().size());
    Assert.assertEquals("k", a.getKey());
    Assert.assertEquals("v", a.getValue());
    // validate that arguments for b did override the global runtime args of the cache
    Assert.assertEquals("k", b.getKey());
    Assert.assertEquals("b", b.getValue());

    // verify that before a tx context is created, tx-awares is empty
    List<TestDataset> txAwares = getTxAwares();
    Assert.assertTrue(txAwares.isEmpty());

    // verify that static datasets are in the tx-awares after start
    TransactionContext txContext = cache.newTransactionContext();
    txAwares = getTxAwares();
    Assert.assertEquals(2, txAwares.size());
    Assert.assertSame(a, txAwares.get(0));
    Assert.assertSame(b, txAwares.get(1));

    // verify that the tx-aware is actually part of the tx context
    txContext.start();
    String sysPropA = System.getProperty("testDataset.a.tx");
    Assert.assertNotNull(sysPropA);
    String sysPropB = System.getProperty("testDataset.b.tx");
    Assert.assertNotNull(sysPropB);

    // and validate that they are in the same tx
    Assert.assertNotEquals(0L, Long.parseLong(sysPropA));
    Assert.assertEquals(sysPropA, sysPropB);

    // get another dataset, validate that it participates in the same tx
    TestDataset c = cache.getDataset("c", ImmutableMap.of("key", "c", "value", "c"));
    String sysPropC = System.getProperty("testDataset.c.tx");
    Assert.assertNotNull(sysPropC);
    Assert.assertEquals(sysPropA, sysPropC);

    // validate runtime arguments of c
    // validate that arguments for b did override the global runtime args of the cache
    Assert.assertEquals("c", c.getKey());
    Assert.assertEquals("c", c.getValue());

    txContext.abort();
    Assert.assertNull(System.getProperty("testDataset.a.tx"));
    Assert.assertNull(System.getProperty("testDataset.b.tx"));
    Assert.assertNull(System.getProperty("testDataset.c.tx"));

    // verify that after a dismiss, tx-awares is empty
    cache.dismissTransactionContext();
    Assert.assertTrue(getTxAwares().isEmpty());

    // attempt to run a GC, and make sure that static datasets were not collected
    // hash codes are based on object identity (TestDataset does not override hashcode)
    int hashA = System.identityHashCode(a);
    int hashB = System.identityHashCode(b);
    //noinspection UnusedAssignment
    a = a1 = a2 = null;
    //noinspection UnusedAssignment
    b = b1 = null;
    //noinspection UnusedAssignment
    c = null;
    // most likely this will not force the collection of the soft references, but we can try
    // TODO (no JIRA) is there a better way to test soft references?
    System.gc();
    a = cache.getDataset("a");
    b = cache.getDataset("b", ImmutableMap.of("value", "b"));
    Assert.assertEquals(System.identityHashCode(a), hashA);
    Assert.assertEquals(System.identityHashCode(b), hashB);

    if (datasetMap != null) {
      datasetMap.put("a", a);
      datasetMap.put("b", b);
    }
  }

  @Test
  public void testThatDatasetsStayInTransaction() throws TransactionFailureException {
    final AtomicInteger hash = new AtomicInteger();
    Transactions.execute(cache.newTransactionContext(), "foo", new Runnable() {
      @Override
      public void run() {
        try {
          // this writes the value "x" to row "key"
          TestDataset ds = cache.getDataset("a", ImmutableMap.of("value", "x"));
          ds.write();
          // the dataset will go out of scope; remember the dataset's hashcode for later comparison
          hash.set(System.identityHashCode(ds));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        // this would collect ds because it is out of scope. But we want to test that the cache maintains a
        // strong reference to it while the transaction is going on
        System.gc();
        try {
          // get the same dataset again. It should be the same object
          TestDataset ds = cache.getDataset("a", ImmutableMap.of("value", "x"));
          Assert.assertEquals(hash.get(), System.identityHashCode(ds));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    // this should collect the dataset instance because it really is out of scope
    System.gc();

    // now validate that the dataset did participate in the commit of the tx
    Transactions.execute(cache.newTransactionContext(), "foo", new Runnable() {
      @Override
      public void run() {
        try {
          TestDataset ds = cache.getDataset("a", ImmutableMap.of("value", "x"));
          Assert.assertEquals("x", ds.read());
          // validate that we now have a different instance because the old one was collected
          Assert.assertNotEquals(hash.get(), System.identityHashCode(ds));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        // validate that only the new instance of the dataset remained active
        Assert.assertEquals(1,
                            Iterables.size(cache.getTransactionAwares())
                              - Iterables.size(cache.getStaticTransactionAwares()));
      }
    });
  }

  private List<TestDataset> getTxAwares() {
    SortedSet<TestDataset> set = new TreeSet<>();
    for (TransactionAware txAware : cache.getTransactionAwares()) {
      TestDataset dataset = (TestDataset) txAware;
      set.add(dataset);
    }
    return ImmutableList.copyOf(set);
  }

}
