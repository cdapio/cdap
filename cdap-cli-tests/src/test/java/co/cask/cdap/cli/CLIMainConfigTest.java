/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.common.conf.CConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for configuring {@link CLIMain}.
 */
public class CLIMainConfigTest {

  @Test
  public void testDefaultCDAPHost() throws Exception {
    CLIConfig cliConfig = new CLIConfig(null);
    CLIMain cliMain = new CLIMain(cliConfig);
    CConfiguration cConf = CConfiguration.create();
    String hostname = cConf.get(co.cask.cdap.common.conf.Constants.Router.ADDRESS);
    Assert.assertEquals(hostname, cliConfig.getHost());
  }

  @Test
  public void testCustomCDAPHost() throws Exception {
    System.setProperty(Constants.EV_HOSTNAME, "sdf");
    CLIConfig cliConfig = new CLIConfig("sdf");
    CLIMain cliMain = new CLIMain(cliConfig);
    Assert.assertEquals("sdf", cliConfig.getHost());
    System.setProperty(Constants.EV_HOSTNAME, "");
  }

}
