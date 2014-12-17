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

package co.cask.cdap.test;

import co.cask.cdap.StandaloneMain;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class StandaloneContainer {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneContainer.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static File tempDirectory;
  private static StandaloneMain standaloneMain;
  /**
   * Index of the current test being run.
   * TODO: Hack to handle when IntegrationTestBase is used as a suite and part of a suite.
   */
  private static int testStackIndex = 0;

  public static void start() throws Exception {
    testStackIndex++;
    if (standaloneMain == null) {
      LOG.info("Starting standalone instance");
      try {
        CConfiguration cConf = CConfiguration.create();
        tempDirectory = Files.createTempDir();
        cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempDirectory.getAbsolutePath());

        // Start without UI
        standaloneMain = StandaloneMain.create(null, cConf, new Configuration());
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

  public static void stop() throws Exception {
    testStackIndex--;
    if (standaloneMain != null && testStackIndex == 0) {
      LOG.info("Stopping standalone instance");
      standaloneMain.shutDown();
      standaloneMain = null;
      try {
        DirUtils.deleteDirectoryContents(tempDirectory);
      } catch (IOException e) {
        LOG.warn("Failed to delete temp directory: " + tempDirectory.getAbsolutePath(), e);
      }
    }
  }
}
