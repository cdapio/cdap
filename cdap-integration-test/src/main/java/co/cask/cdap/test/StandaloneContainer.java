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

package co.cask.cdap.test;

import co.cask.cdap.StandaloneMain;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class StandaloneContainer {

  public static final URI DEFAULT_CONNECTION_URI = URI.create("http://127.0.0.1:10000");

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneContainer.class);

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
        cConf.set(Constants.Router.ADDRESS, "localhost");
        cConf.set(Constants.Dangerous.UNRECOVERABLE_RESET, "true");

        // Start without UI
        standaloneMain = StandaloneMain.create(null, cConf, new Configuration());
        standaloneMain.startUp();

        waitForStandalone(30000);
      } catch (Exception e) {
        LOG.error("Failed to start standalone", e);
        if (standaloneMain != null) {
          standaloneMain.shutDown();
        }

        try {
          DirUtils.deleteDirectoryContents(tempDirectory);
        } catch (IOException ex) {
          LOG.warn("Failed to delete temp directory: " + tempDirectory.getAbsolutePath(), ex);
        }

        throw e;
      }
    }
  }

  private static void waitForStandalone(long timeoutMs) throws UnAuthorizedAccessTokenException,
    InterruptedException, TimeoutException {

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      if (standaloneIsReachable()) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }

    throw new TimeoutException();
  }

  private static boolean standaloneIsReachable() throws UnAuthorizedAccessTokenException {
    MetaClient pingClient = new MetaClient(new ClientConfig.Builder()
                                             .setUri(DEFAULT_CONNECTION_URI)
                                             .build());
    try {
      pingClient.ping();
      return true;
    } catch (IOException e) {
      return false;
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
