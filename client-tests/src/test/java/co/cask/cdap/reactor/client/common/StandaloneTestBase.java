/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.StandaloneMain;
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
public class StandaloneTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneTestBase.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static StandaloneMain standaloneMain;
  /**
   * Index of the current test being run.
   * TODO: Hack to handle when StandaloneTestBase is used as a suite and part of a suite.
   */
  private static int testStackIndex = 0;

  @BeforeClass
  public static void setUpClass() throws Throwable {
    testStackIndex++;
    if (standaloneMain == null) {
      try {
        CConfiguration cConf = CConfiguration.create();
        cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

        // Start without UI
        standaloneMain = StandaloneMain.create(null, cConf, new Configuration());
        standaloneMain.startUp();
      } catch (Throwable e) {
        LOG.error("Failed to start standalone", e);
        if (standaloneMain != null) {
          standaloneMain.shutDown();
        }
        throw e;
      }
    }
  }

  @AfterClass
  public static void tearDownClass() {
    testStackIndex--;
    if (standaloneMain != null && testStackIndex == 0) {
      standaloneMain.shutDown();
      standaloneMain = null;
    }
  }
}
