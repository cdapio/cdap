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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
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

  protected static final NamespaceId NAMESPACE = DatasetFrameworkTestUtil.NAMESPACE_ID;
  protected static final NamespaceId NAMESPACE2 = DatasetFrameworkTestUtil.NAMESPACE2_ID;
  protected static DatasetFramework dsFramework;
  protected static TransactionSystemClient txClient;
  protected DynamicDatasetCache cache;

  @BeforeClass
  public static void init() throws DatasetManagementException, IOException {
    dsFramework = dsFrameworkUtil.getFramework();
    dsFramework.addModule(NAMESPACE.datasetModule("testDataset"), new TestDatasetModule());
    dsFramework.addModule(NAMESPACE2.datasetModule("testDataset"), new TestDatasetModule());
    txClient = new InMemoryTxSystemClient(dsFrameworkUtil.getTxManager());
    dsFrameworkUtil.createInstance("testDataset", NAMESPACE.dataset("a"), DatasetProperties.EMPTY);
    dsFrameworkUtil.createInstance("testDataset", NAMESPACE.dataset("b"), DatasetProperties.EMPTY);
    dsFrameworkUtil.createInstance("testDataset", NAMESPACE.dataset("c"), DatasetProperties.EMPTY);
    dsFrameworkUtil.createInstance("testDataset", NAMESPACE2.dataset("a2"), DatasetProperties.EMPTY);
    dsFrameworkUtil.createInstance("testDataset", NAMESPACE2.dataset("c2"), DatasetProperties.EMPTY);
  }

  @AfterClass
  public static void tearDown() throws IOException, DatasetManagementException {
    dsFrameworkUtil.deleteInstance(NAMESPACE.dataset("a"));
    dsFrameworkUtil.deleteInstance(NAMESPACE.dataset("b"));
    dsFrameworkUtil.deleteInstance(NAMESPACE.dataset("c"));
    dsFrameworkUtil.deleteInstance(NAMESPACE2.dataset("a2"));
    dsFrameworkUtil.deleteInstance(NAMESPACE2.dataset("c2"));
    dsFrameworkUtil.deleteModule(NAMESPACE.datasetModule("testDataset"));
    dsFrameworkUtil.deleteModule(NAMESPACE2.datasetModule("testDataset"));
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
    TestDataset b1 = cache.getDataset("b", B_ARGUMENTS, AccessType.READ);
    TestDataset b2 = cache.getDataset("b", B_ARGUMENTS, AccessType.WRITE);
    Assert.assertSame(b, b1);
    // assert that b1 and b2 are the same, even though their accessType is different
    Assert.assertSame(b1, b2);

    // Test this for cross namespace access
    TestDataset an2 = cache.getDataset(NAMESPACE2.getEntityName(), "a2");
    TestDataset a1n2 = cache.getDataset(NAMESPACE2.getEntityName(), "a2");
    TestDataset a2n2 = cache.getDataset(NAMESPACE2.getEntityName(), "a2", A_ARGUMENTS);
    Assert.assertSame(an2, a1n2);
    Assert.assertSame(an2, a2n2);
    Assert.assertNotSame(a, an2);


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
    Assert.assertEquals(3, txAwares.size());
    Assert.assertTrue(verifyTxAwaresContains(txAwares, a, an2, b));

    // verify that the tx-aware is actually part of the tx context and that they are in the same tx
    txContext.start();
    Assert.assertNotNull(a.getCurrentTransaction());
    Assert.assertNotEquals(0L, a.getCurrentTransaction().getWritePointer());
    Assert.assertEquals(a.getCurrentTransaction(), b.getCurrentTransaction());

    // get another dataset, validate that it participates in the same tx
    TestDataset c = cache.getDataset("c", C_ARGUMENTS);
    Assert.assertEquals(a.getCurrentTransaction(), c.getCurrentTransaction());

    // another dataset from different namespace
    TestDataset cn2 = cache.getDataset(NAMESPACE2.getEntityName(), "c2", C_ARGUMENTS);
    Assert.assertEquals(an2.getCurrentTransaction(), cn2.getCurrentTransaction());

    // validate runtime arguments of c and c2
    // validate that arguments for b did override the global runtime args of the cache
    Assert.assertEquals("c", c.getKey());
    Assert.assertEquals("c", c.getValue());

    Assert.assertEquals("c", cn2.getKey());
    Assert.assertEquals("c", cn2.getValue());

    // validate that c was added to the tx-awares
    txAwares = getTxAwares();
    Assert.assertEquals(5, txAwares.size());
    Assert.assertTrue(verifyTxAwaresContains(txAwares, a, an2, b, c, cn2));

    // discard b and c and c2, validate that they are not closed yet
    cache.discardDataset(b);
    cache.discardDataset(c);
    cache.discardDataset(cn2);
    Assert.assertFalse(b.isClosed());
    Assert.assertFalse(c.isClosed());
    Assert.assertFalse(cn2.isClosed());

    // validate that static dataset b is still in the tx-awares, whereas c was dropped
    txAwares = getTxAwares();
    Assert.assertEquals(3, txAwares.size());
    Assert.assertTrue(verifyTxAwaresContains(txAwares, a, an2, b));

    // get b and c again, validate that they are the same
    TestDataset b3 = cache.getDataset("b", B_ARGUMENTS);
    TestDataset c1 = cache.getDataset("c", C_ARGUMENTS);
    TestDataset c1n2 = cache.getDataset(NAMESPACE2.getEntityName(), "c2", C_ARGUMENTS);
    Assert.assertSame(b3, b);
    Assert.assertSame(c1, c);
    Assert.assertSame(c1n2, cn2);

    // validate that c and c2 is back in the tx-awares
    txAwares = getTxAwares();
    Assert.assertEquals(5, txAwares.size());
    Assert.assertTrue(verifyTxAwaresContains(txAwares, a, an2, b, c, cn2));

    // discard b and c and c2, validate that they are not closed yet
    cache.discardDataset(b);
    cache.discardDataset(c);
    cache.discardDataset(cn2);
    Assert.assertFalse(b.isClosed());
    Assert.assertFalse(c.isClosed());
    Assert.assertFalse(cn2.isClosed());

    // validate that static dataset b is still in the tx-awares, whereas c was dropped
    txAwares = getTxAwares();
    Assert.assertEquals(3, txAwares.size());
    Assert.assertTrue(verifyTxAwaresContains(txAwares, a, an2, b));

    // validate that all datasets participate in abort of tx
    txContext.abort();
    Assert.assertNull(a.getCurrentTransaction());
    Assert.assertNull(b.getCurrentTransaction());
    Assert.assertNull(c.getCurrentTransaction());
    Assert.assertNull(an2.getCurrentTransaction());
    Assert.assertNull(cn2.getCurrentTransaction());

    // validate that c disappears from the txAwares after tx aborted, and that it was closed
    txAwares = getTxAwares();
    Assert.assertEquals(3, txAwares.size());
    Assert.assertTrue(verifyTxAwaresContains(txAwares, a, an2, b));
    Assert.assertTrue(c.isClosed());
    Assert.assertTrue(cn2.isClosed());

    // but also that b (a static dataset) remains and was not closed
    Assert.assertFalse(b.isClosed());

    // validate the tx context does not include c in the next transaction
    txContext.start();
    Assert.assertNotNull(a.getCurrentTransaction());
    Assert.assertNotNull(an2.getCurrentTransaction());
    Assert.assertEquals(a.getCurrentTransaction(), b.getCurrentTransaction());
    Assert.assertNull(c.getCurrentTransaction());
    Assert.assertNull(cn2.getCurrentTransaction());

    // validate that discarding a dataset that is not in the tx does not cause errors
    cache.discardDataset(c);
    cache.discardDataset(cn2);

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
    Transactions.execute(cache.newTransactionContext(), "foo", () -> {
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
    });

    // now validate that the dataset did participate in the commit of the tx
    Transactions.execute(cache.newTransactionContext(), "foo", () -> {
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
    });
  }

  private Boolean verifyTxAwaresContains(List<TestDataset> txAwares, TestDataset... datasets) {
    for (TestDataset dataset : datasets) {
      if (!txAwares.contains(dataset)) {
        return false;
      }
    }
    return true;
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
