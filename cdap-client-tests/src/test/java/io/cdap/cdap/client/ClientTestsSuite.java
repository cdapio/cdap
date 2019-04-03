/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.test.TestSuite;
import io.cdap.cdap.test.XSlowTests;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all client tests.
 */
@Category(XSlowTests.class)
@RunWith(TestSuite.class)
@Suite.SuiteClasses({
  ApplicationClientTestRun.class,
  ArtifactClientTestRun.class,
  DatasetClientTestRun.class,
  LineageHttpHandlerTestRun.class,
  MetaClientTestRun.class,
  MetadataHttpHandlerTestRun.class,
  MetricsClientTestRun.class,
  MonitorClientTestRun.class,
  NamespaceClientTestRun.class,
  PreferencesClientTestRun.class,
  ProgramClientTestRun.class,
  ScheduleClientTestRun.class,
  ServiceClientTestRun.class,
  UsageHandlerTestRun.class,
  WorkflowClientTestRun.class,
})
public class ClientTestsSuite extends ClientTestBase {

}
