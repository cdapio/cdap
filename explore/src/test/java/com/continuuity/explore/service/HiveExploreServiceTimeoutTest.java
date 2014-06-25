package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests timeout of operations in HiveExploreService.
 */
@Category(SlowTests.class)
public class HiveExploreServiceTimeoutTest extends BaseHiveExploreServiceTest {
  private static final long ACTIVE_OPERATION_TIMEOUT_SECS = 5;
  private static final long INACTIVE_OPERATION_TIMEOUT_SECS = 1;
  private static final long CLEANUP_JOB_SCHEDULE_SECS = 1;

  private static ExploreService exploreService;

  @BeforeClass
  public static void start() throws Exception {
    // Set smaller values for timeouts for testing
    CConfiguration cConfiguration = CConfiguration.create();
    cConfiguration.setLong(Constants.Explore.ACTIVE_OPERATION_TIMEOUT_SECS, ACTIVE_OPERATION_TIMEOUT_SECS);
    cConfiguration.setLong(Constants.Explore.INACTIVE_OPERATION_TIMEOUT_SECS, INACTIVE_OPERATION_TIMEOUT_SECS);
    cConfiguration.setLong(Constants.Explore.CLEANUP_JOB_SCHEDULE_SECS, CLEANUP_JOB_SCHEDULE_SECS);

    startServices(cConfiguration);

    exploreService = injector.getInstance(ExploreService.class);

    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table", DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    table.startTx(tx1);

    KeyStructValueTableDefinition.KeyValue.Value value1 =
      new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyStructValueTableDefinition.KeyValue.Value value2 =
      new KeyStructValueTableDefinition.KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals(value1, table.get("1"));
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteModule("keyStructValue");
  }

  @Test
  public void testTimeoutRunning() throws Exception {
    Set<Long> beforeTxns = transactionManager.getCurrentState().getInProgress().keySet();

    Handle handle = exploreService.execute("select key, value from continuuity_user_my_table");

    Set<Long> queryTxns = Sets.difference(transactionManager.getCurrentState().getInProgress().keySet(), beforeTxns);
    Assert.assertFalse(queryTxns.isEmpty());

    // Get status should work here
    exploreService.getStatus(handle);

    // Sleep for timeout to happen
    TimeUnit.SECONDS.sleep(ACTIVE_OPERATION_TIMEOUT_SECS + 3);

    try {
      exploreService.getStatus(handle);
      Assert.fail("Should throw HandleNotFoundException due to operation timeout");
    } catch (HandleNotFoundException e) {
      // Expected exception due to timeout
    }

    // Make sure that the transaction got closed
    Assert.assertEquals(ImmutableSet.<Long>of(),
                        Sets.intersection(
                          queryTxns,
                          transactionManager.getCurrentState().getInProgress().keySet()).immutableCopy()
    );
  }

  @Test
  public void testTimeoutFetchAllResults() throws Exception {
    Set<Long> beforeTxns = transactionManager.getCurrentState().getInProgress().keySet();

    Handle handle = exploreService.execute("select key, value from continuuity_user_my_table");

    Set<Long> queryTxns = Sets.difference(transactionManager.getCurrentState().getInProgress().keySet(), beforeTxns);
    Assert.assertFalse(queryTxns.isEmpty());

    Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 20);
    Assert.assertEquals(Status.OpStatus.FINISHED, status.getStatus());
    Assert.assertTrue(status.hasResults());

    // Fetch all results
    while (!exploreService.nextResults(handle, 100).isEmpty()) {
      // nothing to do
    }

    // Get status should work here
    exploreService.getStatus(handle);

    // Sleep for timeout to happen
    TimeUnit.SECONDS.sleep(INACTIVE_OPERATION_TIMEOUT_SECS + 3);

    try {
      exploreService.getStatus(handle);
      Assert.fail("Should throw HandleNotFoundException due to operation timeout");
    } catch (HandleNotFoundException e) {
      // Expected exception due to timeout
    }

    // Make sure that the transaction got closed
    Assert.assertEquals(ImmutableSet.<Long>of(),
                        Sets.intersection(
                          queryTxns, transactionManager.getCurrentState().getInProgress().keySet()).immutableCopy()
    );
  }

  @Test
  public void testTimeoutCancel() throws Exception {
    Set<Long> beforeTxns = transactionManager.getCurrentState().getInProgress().keySet();

    Handle handle = exploreService.execute("select key, value from continuuity_user_my_table");

    Set<Long> queryTxns = Sets.difference(transactionManager.getCurrentState().getInProgress().keySet(), beforeTxns);
    Assert.assertFalse(queryTxns.isEmpty());

    exploreService.cancel(handle);

    // Get status should work here
    exploreService.getStatus(handle);

    // Sleep for timeout to happen
    TimeUnit.SECONDS.sleep(INACTIVE_OPERATION_TIMEOUT_SECS + 3);

    try {
      exploreService.getStatus(handle);
      Assert.fail("Should throw HandleNotFoundException due to operation timeout");
    } catch (HandleNotFoundException e) {
      // Expected exception due to timeout
    }

    // Make sure that the transaction got closed
    Assert.assertEquals(ImmutableSet.<Long>of(),
                        Sets.intersection(
                          queryTxns,
                          transactionManager.getCurrentState().getInProgress().keySet()).immutableCopy()
    );
  }

  @Test
  public void testTimeoutNoResults() throws Exception {
    Set<Long> beforeTxns = transactionManager.getCurrentState().getInProgress().keySet();

    Handle handle = exploreService.execute("drop table if exists not_existing_table_name");

    Set<Long> queryTxns = Sets.difference(transactionManager.getCurrentState().getInProgress().keySet(), beforeTxns);
    Assert.assertFalse(queryTxns.isEmpty());

    Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 20);
    Assert.assertEquals(Status.OpStatus.FINISHED, status.getStatus());

    // Get status should work here
    exploreService.getStatus(handle);

    // Sleep for timeout to happen
    TimeUnit.SECONDS.sleep(INACTIVE_OPERATION_TIMEOUT_SECS + 3);

    try {
      exploreService.getStatus(handle);
      Assert.fail("Should throw HandleNotFoundException due to operation timeout");
    } catch (HandleNotFoundException e) {
      // Expected exception due to timeout
    }

    // Make sure that the transaction got closed
    Assert.assertEquals(ImmutableSet.<Long>of(),
                        Sets.intersection(
                          queryTxns,
                          transactionManager.getCurrentState().getInProgress().keySet()).immutableCopy()
    );
  }
}
