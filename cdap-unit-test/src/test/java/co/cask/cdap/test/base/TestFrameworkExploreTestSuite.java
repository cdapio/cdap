/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.base;

import co.cask.cdap.common.test.TestSuite;
import co.cask.cdap.partitioned.PartitionConsumingTestRun;
import co.cask.cdap.partitioned.PartitionCorrectorTestRun;
import co.cask.cdap.partitioned.PartitionRollbackTestRun;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.app.DummyBaseCloneTestRun;
import co.cask.cdap.test.app.DummyBaseTestRun;
import co.cask.cdap.test.app.DynamicPartitioningTestRun;
import co.cask.cdap.test.app.TestSQLQueryTestRun;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This is a test suite that runs all tests in the cdap-unit-test module that require explore to be enabled.
 * This avoid starting/stopping app-fabric per test.
 */
@Category(XSlowTests.class)
@RunWith(TestSuite.class)
@Suite.SuiteClasses({
  DummyBaseTestRun.class,
  DummyBaseCloneTestRun.class,
  DynamicPartitioningTestRun.class,
  PartitionConsumingTestRun.class,
  PartitionCorrectorTestRun.class,
  PartitionRollbackTestRun.class,
  TestSQLQueryTestRun.class
})
public class TestFrameworkExploreTestSuite extends TestFrameworkTestBase {

}
