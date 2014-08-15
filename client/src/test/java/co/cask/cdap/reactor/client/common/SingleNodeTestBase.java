/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.reactor.client.common;

import co.cask.cdap.SingleNodeMain;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SingleNodeTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeTestBase.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static SingleNodeMain singleNodeMain;
  /**
   * Index of the current test being run.
   * TODO: Hack to handle when SingleNodeTestBase is used as a suite and part of a suite.
   */
  private static int testStackIndex = 0;

  @BeforeClass
  public static void setUpClass() throws Throwable {
    testStackIndex++;
    if (singleNodeMain == null) {
      try {
        CConfiguration cConf = CConfiguration.create();
        cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

        // Start singlenode without UI
        singleNodeMain = SingleNodeMain.createSingleNodeMain(true, null, cConf, new Configuration());
        singleNodeMain.startUp();
      } catch (Throwable e) {
        LOG.error("Failed to start singlenode", e);
        if (singleNodeMain != null) {
          singleNodeMain.shutDown();
        }
        throw e;
      }
    }
  }

  @AfterClass
  public static void tearDownClass() {
    testStackIndex--;
    if (singleNodeMain != null && testStackIndex == 0) {
      singleNodeMain.shutDown();
      singleNodeMain = null;
    }
  }
}
