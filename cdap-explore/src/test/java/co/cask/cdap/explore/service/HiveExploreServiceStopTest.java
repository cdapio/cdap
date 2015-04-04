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

package co.cask.cdap.explore.service;

import co.cask.tephra.persist.TransactionSnapshot;
import co.cask.tephra.persist.TransactionStateStorage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Set;

/**
 * Tests whether txns get closed on stopping explore service.
 */
public class HiveExploreServiceStopTest extends BaseHiveExploreServiceTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void start() throws Exception {
    // Services are stopped in test testServiceStop, hence no need to stop in AfterClass
    BaseHiveExploreServiceTest.runAfter = false;
    initialize(tmpFolder);
  }

  @AfterClass
  public static void afterClass() {
    BaseHiveExploreServiceTest.runAfter = true;
  }

  @Test
  public void testServiceStop() throws Exception {
    ExploreService exploreService = injector.getInstance(ExploreService.class);
    Set<Long> beforeTxns = transactionManager.getCurrentState().getInProgress().keySet();

    exploreService.execute(NAMESPACE_ID, "show tables");

    Set<Long> queryTxns = Sets.difference(transactionManager.getCurrentState().getInProgress().keySet(), beforeTxns);
    Assert.assertFalse(queryTxns.isEmpty());

    // Stop all services so that explore service gets stopped.
    stopServices();

    // Make sure that the transaction got closed
    TransactionStateStorage transactionStateStorage = injector.getInstance(TransactionStateStorage.class);
    TransactionSnapshot latestSnapshot = transactionStateStorage.getLatestSnapshot();
    Assert.assertEquals(ImmutableSet.<Long>of(),
                        Sets.intersection(
                          queryTxns,
                          latestSnapshot.getInProgress().keySet()).immutableCopy()
    );
  }

}
