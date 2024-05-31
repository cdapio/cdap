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

package io.cdap.cdap.hbase.wd;

import io.cdap.cdap.test.XSlowTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@Category(XSlowTests.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({
  IdentityHashDistributorTestRun.class,
  MultiBytesPrefixHashDistributorTestRun.class,
  OneByteSimpleHashDistributorTestRun.class,
  RowKeyDistributorByOneBytePrefixTestRun.class
})
public class DistributorTestsSuite {

  @BeforeClass
  public static void init() throws Exception {
    RowKeyDistributorTestBase.beforeClass();
    RowKeyDistributorTestBase.runBefore = false;
    RowKeyDistributorTestBase.runAfter = false;
  }

  @AfterClass
  public static void finish() throws Exception {
    RowKeyDistributorTestBase.runAfter = true;
    RowKeyDistributorTestBase.afterClass();
  }
}
