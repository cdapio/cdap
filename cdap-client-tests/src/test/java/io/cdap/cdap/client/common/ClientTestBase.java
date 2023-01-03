/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.client.common;

import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.client.AbstractClientTest;
import io.cdap.cdap.common.test.TestRunner;
import io.cdap.cdap.test.SingletonExternalResource;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Common base test class for all client unit-tests that doesn't need explore service.
 */
@RunWith(TestRunner.class)
public abstract class ClientTestBase extends AbstractClientTest {

  @ClassRule
  public static final SingletonExternalResource STANDALONE = new SingletonExternalResource(
    new StandaloneTester());

  @Override
  protected StandaloneTester getStandaloneTester() {
    return STANDALONE.get();
  }
}
