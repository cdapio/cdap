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

package co.cask.cdap.data.startup;

import co.cask.cdap.common.conf.CConfiguration;
import org.apache.tephra.TxConstants;
import org.junit.Test;

public class TransactionServiceCheckTest {

  protected void run(CConfiguration cConf) throws Exception {
    new TransactionServiceCheck(cConf).run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultNotConfigured() throws Exception {
    CConfiguration cConfiguration = CConfiguration.create();
    cConfiguration.unset(TxConstants.Manager.CFG_TX_TIMEOUT);
    run(cConfiguration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultNotInteger() throws Exception {
    CConfiguration cConfiguration = CConfiguration.create();
    cConfiguration.set(TxConstants.Manager.CFG_TX_TIMEOUT, "NaN");
    run(cConfiguration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxNotConfigured() throws Exception {
    CConfiguration cConfiguration = CConfiguration.create();
    cConfiguration.unset(TxConstants.Manager.CFG_TX_MAX_TIMEOUT);
    run(cConfiguration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxNotInteger() throws Exception {
    CConfiguration cConfiguration = CConfiguration.create();
    cConfiguration.set(TxConstants.Manager.CFG_TX_MAX_TIMEOUT, "NaN");
    run(cConfiguration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDefaultExceedsMax() throws Exception {
    CConfiguration cConfiguration = CConfiguration.create();
    cConfiguration.setInt(TxConstants.Manager.CFG_TX_TIMEOUT,
                          cConfiguration.getInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT) + 5);
    run(cConfiguration);
  }
}
