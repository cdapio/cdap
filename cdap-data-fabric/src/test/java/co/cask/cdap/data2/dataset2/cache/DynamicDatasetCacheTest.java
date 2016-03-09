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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
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
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

public abstract class DynamicDatasetCacheTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Map<String, String> ARGUMENTS = ImmutableMap.of("key", "k", "value", "v");
  private static final Map<String, String> NO_ARGUMENTS = ImmutableMap.of();
  private static final Map<String, String> A_ARGUMENTS = NO_ARGUMENTS;
  private static final Map<String, String> B_ARGUMENTS = ImmutableMap.of("value", "b");
  private static final Map<String, String> C_ARGUMENTS = ImmutableMap.of("key", "c", "value", "c");
  private static final Map<String, String> X_ARGUMENTS = ImmutableMap.of("value", "x");

  protected static final Id.Namespace NAMESPACE = DatasetFrameworkTestUtil.NAMESPACE_ID;
  protected static final NamespaceId NAMESPACE_ID = new NamespaceId(NAMESPACE.getId());
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
    cache = createCache(instantiator, ARGUMENTS, ImmutableMap.of("b", B_ARGUMENTS));
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

    // test that getting the same dataset are always the same object
    TestDataset a = cache.getDataset("a");
    TestDataset a1 = cache.getDataset("a");
    TestDataset a2 = cache.getDataset("a", A_ARGUMENTS);
    Assert.assertSame(a, a1);
    Assert.assertSame(a, a2);

    TestDataset b = cache.getDataset("b", B_ARGUMENTS);
    TestDataset b1 = cache.getDataset("b", B_ARGUMENTS);
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

    // verify that the tx-aware is actually part of the tx context and that they are in the same tx
    txContext.start();
    Assert.assertNotNull(a.getCurrentTransaction());
    Assert.assertNotEquals(0L, a.getCurrentTransaction().getWritePointer());
    Assert.assertEquals(a.getCurrentTransaction(), b.getCurrentTransaction());

    // get another dataset, validate that it participates in the same tx
    TestDataset c = cache.getDataset("c", C_ARGUMENTS);
    Assert.assertEquals(a.getCurrentTransaction(), c.getCurrentTransaction());

    // validate runtime arguments of c
    // validate that arguments for b did override the global runtime args of the cache
    Assert.assertEquals("c", c.getKey());
    Assert.assertEquals("c", c.getValue());

    // validate that c was added to the tx-awares
    txAwares = getTxAwares();
    Assert.assertEquals(3, txAwares.size());
    Assert.assertSame(a, txAwares.get(0));
    Assert.assertSame(b, txAwares.get(1));
    Assert.assertSame(c, txAwares.get(2));

    // discard b and c, validate that they are not closed yet
    cache.discardDataset(b);
    cache.discardDataset(c);
    Assert.assertFalse(b.isClosed());
    Assert.assertFalse(c.isClosed());

    // validate that static dataset b is still in the tx-awares, whereas c was dropped
    txAwares = getTxAwares();
    Assert.assertEquals(2, txAwares.size());
    Assert.assertSame(a, txAwares.get(0));
    Assert.assertSame(b, txAwares.get(1));

    // get b and c again, validate that they are the same
    TestDataset b3 = cache.getDataset("b", B_ARGUMENTS);
    TestDataset c1 = cache.getDataset("c", C_ARGUMENTS);
    Assert.assertSame(b3, b);
    Assert.assertSame(c1, c);

    // validate that c is back in the tx-awares
    txAwares = getTxAwares();
    Assert.assertEquals(3, txAwares.size());
    Assert.assertSame(a, txAwares.get(0));
    Assert.assertSame(b, txAwares.get(1));
    Assert.assertSame(c, txAwares.get(2));

    // discard b and c, validate that they are not closed yet
    cache.discardDataset(b);
    cache.discardDataset(c);
    Assert.assertFalse(b.isClosed());
    Assert.assertFalse(c.isClosed());

    // validate that static dataset b is still in the tx-awares, whereas c was dropped
    txAwares = getTxAwares();
    Assert.assertEquals(2, txAwares.size());
    Assert.assertSame(a, txAwares.get(0));
    Assert.assertSame(b, txAwares.get(1));

    // validate that all datasets participate in abort of tx
    txContext.abort();
    Assert.assertNull(a.getCurrentTransaction());
    Assert.assertNull(b.getCurrentTransaction());
    Assert.assertNull(c.getCurrentTransaction());

    // validate that c disappears from the txAwares after tx aborted, and that it was closed
    txAwares = getTxAwares();
    Assert.assertEquals(2, txAwares.size());
    Assert.assertSame(a, txAwares.get(0));
    Assert.assertSame(b, txAwares.get(1));
    Assert.assertTrue(c.isClosed());

    // but also that b (a static dataset) remains and was not closed
    Assert.assertFalse(b.isClosed());

    // validate the tx context does not include c in the next transaction
    txContext.start();
    Assert.assertNotNull(a.getCurrentTransaction());
    Assert.assertEquals(a.getCurrentTransaction(), b.getCurrentTransaction());
    Assert.assertNull(c.getCurrentTransaction());

    // validate that discarding a dataset that is not in the tx does not cause errors
    cache.discardDataset(c);

    // get a new instance of c, validate that it is not the same as before, that is, c was really discarded
    c1 = cache.getDataset("c", C_ARGUMENTS);
    Assert.assertNotSame(c, c1);

    // discard c and finish the tx
    cache.discardDataset(c1);
    txContext.finish();

    // verify that after discarding the tx context, tx-awares is empty
    cache.dismissTransactionContext();
    Assert.assertTrue(getTxAwares().isEmpty());

    // verify that after getting an new tx-context, we still have the same datasets
    txContext = cache.newTransactionContext();
    txContext.start();
    Assert.assertNotNull(txContext.getCurrentTransaction());
    Assert.assertEquals(txContext.getCurrentTransaction(), a.getCurrentTransaction());
    Assert.assertEquals(txContext.getCurrentTransaction(), b.getCurrentTransaction());
    txContext.abort();

    if (datasetMap != null) {
      datasetMap.put("a", a);
      datasetMap.put("b", b);
    }
  }

  @Test
  public void testThatDatasetsStayInTransaction() throws TransactionFailureException {
    final AtomicReference<Object> ref = new AtomicReference<>();
    Transactions.execute(cache.newTransactionContext(), "foo", new Runnable() {
      @Override
      public void run() {
        try {
          // this writes the value "x" to row "key"
          TestDataset ds = cache.getDataset("a", X_ARGUMENTS);
          ds.write();
          // this would close and discard ds, but the transaction is going on, so we should get the
          // identical instance of the dataset again
          cache.discardDataset(ds);
          TestDataset ds2 = cache.getDataset("a", X_ARGUMENTS);
          Assert.assertSame(ds, ds2);
          ref.set(ds);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        try {
          // get the same dataset again. It should be the same object
          TestDataset ds = cache.getDataset("a", X_ARGUMENTS);
          Assert.assertSame(ref.get(), ds);
          cache.discardDataset(ds);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    // now validate that the dataset did participate in the commit of the tx
    Transactions.execute(cache.newTransactionContext(), "foo", new Runnable() {
      @Override
      public void run() {
        try {
          TestDataset ds = cache.getDataset("a", X_ARGUMENTS);
          Assert.assertEquals("x", ds.read());
          // validate that we now have a different instance because the old one was discarded
          Assert.assertNotSame(ref.get(), ds);
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
