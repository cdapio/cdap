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

package co.cask.cdap.explore.service;

import co.cask.cdap.proto.id.NamespaceId;
import org.apache.hive.service.cli.HiveSQLException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;

/**
 * Tests whether txns get invalidated on failed read-write txn.
 */
public class HiveExploreServiceInvalidateTxTest extends BaseHiveExploreServiceTest {

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
  public void testInvalidateTx() throws Exception {
    ExploreService exploreService = injector.getInstance(ExploreService.class);

    // check that BaseHiveExploreService invalidates read-write transaction that failed
    Assert.assertEquals(0, transactionManager.getCurrentState().getInvalid().size());
    try {
      waitForCompletionStatus(exploreService.execute(NAMESPACE_ID, "select * from dataset_nonexistent"),
                              5, TimeUnit.SECONDS, 10);
      Assert.fail("Expected HiveSQLException");
    } catch (HiveSQLException e) {
      // Expected
    }
    Assert.assertEquals(1, transactionManager.getCurrentState().getInvalid().size());

    try {
      waitForCompletionStatus(exploreService.deleteNamespace(new NamespaceId("nonexistent")), 5, TimeUnit.SECONDS, 10);
      Assert.fail("Expected HiveSQLException");
    } catch (HiveSQLException e) {
      // Expected
    }
    Assert.assertEquals(1, transactionManager.getCurrentState().getInvalid().size());
  }

}
