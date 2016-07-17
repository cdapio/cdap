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

package co.cask.cdap.client.common;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.client.AbstractClientTest;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.test.TestRunner;
import co.cask.cdap.test.SingletonExternalResource;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Common base test class for all client unit-tests that doesn't need explore service.
 */
@RunWith(TestRunner.class)
public abstract class ClientTestBase extends AbstractClientTest {

  @ClassRule
  public static final SingletonExternalResource STANDALONE = new SingletonExternalResource(
    new StandaloneTester(Constants.Explore.EXPLORE_ENABLED, false));

  @Override
  protected StandaloneTester getStandaloneTester() {
    return STANDALONE.get();
  }
}
