/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.test.standalone;

import co.cask.cdap.StandaloneContainer;
import co.cask.cdap.StandaloneMain;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
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
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  protected static CConfiguration configuration;

  private static StandaloneMain standaloneMain;
  /**
   * Index of the current test being run.
   * TODO: Hack to handle when StandaloneTestBase is used as a suite and part of a suite.
   */
  private static int testStackIndex = 0;

  @BeforeClass
  public static void setUpClass() throws Exception {
    testStackIndex++;
    if (standaloneMain == null) {
      try {
        if (configuration == null) {
          configuration = CConfiguration.create();
        }
        configuration.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

        // Start without UI
        standaloneMain = StandaloneMain.create(null, configuration, new Configuration());
        standaloneMain.startUp();

      } catch (Exception e) {
        LOG.error("Failed to start standalone", e);
        if (standaloneMain != null) {
          standaloneMain.shutDown();
        }
        throw e;
      }
    }
  }

  @After
  public void tearDownStandalone() throws Exception {
    ProgramClient programClient = new ProgramClient(getClientConfig());
    programClient.stopAll();
    MetaClient metaClient = new MetaClient(getClientConfig());
    metaClient.resetUnrecoverably();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testStackIndex--;
    if (standaloneMain != null && testStackIndex == 0) {
      standaloneMain.shutDown();
      standaloneMain = null;
      LevelDBTableService.getInstance().clearTables();
    }
  }

  protected ClientConfig getClientConfig() {
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(
      StandaloneContainer.DEFAULT_CONNECTION_URI.toString());

    ClientConfig.Builder builder = new ClientConfig.Builder();
    builder.setConnectionConfig(connectionConfig);
    builder.setDefaultConnectTimeout(120000);
    builder.setDefaultReadTimeout(120000);
    builder.setUploadConnectTimeout(0);
    builder.setUploadConnectTimeout(0);

    return builder.build();
  }
}
