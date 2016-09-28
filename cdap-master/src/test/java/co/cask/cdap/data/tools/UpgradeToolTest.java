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

package co.cask.cdap.data.tools;

import com.google.inject.Injector;
import org.junit.Test;

/**
 *
 */
public class UpgradeToolTest {
  @Test
  public void testInjector() throws Exception {
    Injector upgradeToolInjector = new UpgradeTool().createInjector();
    // should not throw exception
    // Test the UpgradeDatasetServiceManager injector creation
    upgradeToolInjector.getInstance(UpgradeDatasetServiceManager.class);
    // should not throw exception
  }
}
