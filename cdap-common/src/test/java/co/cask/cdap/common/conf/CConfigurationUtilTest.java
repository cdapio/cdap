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

package co.cask.cdap.common.conf;

import org.junit.Assert;
import org.junit.Test;

public class CConfigurationUtilTest {

  @Test
  public void testCheckCConfValidity() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    assertIsValidCConf(cConf);

    // test some invalid values (and combinations)
    cConf.set(Constants.CFG_ROOT_NAMESPACE, "invalid_root");
    assertIsInvalidCConf(cConf);
    cConf.set(Constants.CFG_ROOT_NAMESPACE, "invalid.root");
    assertIsInvalidCConf(cConf);
    cConf.set(Constants.Dataset.TABLE_PREFIX, "invalid/root");
    assertIsInvalidCConf(cConf);
    cConf.set(Constants.CFG_ROOT_NAMESPACE, "invalid\root");
    assertIsInvalidCConf(cConf);

    // set the invalid values back to valid values
    cConf.set(Constants.CFG_ROOT_NAMESPACE, "cdap");
    cConf.set(Constants.Dataset.TABLE_PREFIX, "dsprefix");
    assertIsValidCConf(cConf);

    cConf.set(Constants.CFG_ROOT_NAMESPACE, "cdap1");
    assertIsValidCConf(cConf);

    // test additional invalid values (and combinations)
    cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.TABLE_PREFIX, "invalid.table.prefix");
    assertIsInvalidCConf(cConf);

    cConf.set(Constants.CFG_ROOT_NAMESPACE, "invalid/root/prefix");
    assertIsInvalidCConf(cConf);
  }

  private void assertIsInvalidCConf(CConfiguration cConf) {
    try {
      CConfigurationUtil.checkCConfValidity(cConf);
      Assert.fail("Expected cConf to be invalid");
    } catch (IllegalArgumentException expected) {
    }
  }

  private void assertIsValidCConf(CConfiguration cConf) {
    CConfigurationUtil.checkCConfValidity(cConf);
  }
}
