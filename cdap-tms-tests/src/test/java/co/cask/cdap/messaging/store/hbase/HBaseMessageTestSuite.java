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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.test.SingletonExternalResource;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  HBaseMetadataTableTestRun.class,
  HBaseMessageTableTestRun.class,
  HBasePayloadTableTestRun.class,
  HBaseTableCoprocessorTestRun.class
})
public class HBaseMessageTestSuite {

  static final HBaseTestBase HBASE_TEST_BASE = new HBaseTestFactory().get();

  // This is used by JUnit only. Wrap it with SingletonExternalResource so that it only get started once in the suite
  // Individual test class included in this suite will also have a @ClassRule that point to this field.
  @ClassRule
  public static final ExternalResource TEST_BASE = new SingletonExternalResource(HBASE_TEST_BASE);
}
