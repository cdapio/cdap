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
package co.cask.cdap;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Configuration;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.ui.ConfigurationJsonTool;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * UserInterfaceService is a basic Server wrapper that launches node.js and our
 * UI server.js file. It then basically sits there waiting, doing nothing.
 *
 * All output is sent to our Logging service.
 */
final class UserInterfaceService extends AbstractExecutionThreadService {

  private static final String JSON_PATH = "cdap-config.json";
  private static final String JSON_SECURITY_PATH = "cdap-security-config.json";

  private static final Logger LOG = LoggerFactory.getLogger(UserInterfaceService.class);
  private static final String NODE_JS_EXECUTABLE = "node";

  private final CConfiguration cConf;
  private final SConfiguration sConf;

  private Process process;
  private BufferedReader bufferedReader;

  @Nullable
  private File cConfJsonFile;

  @Nullable
  private File sConfJsonFile;

  @Inject
  UserInterfaceService(CConfiguration cConf, SConfiguration sConf) {
    this.cConf = cConf;
    this.sConf = sConf;
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {
    File confDir = new File("data", "conf");
    if (!confDir.exists()) {
      Preconditions.checkState(confDir.mkdirs(), "Couldn't create directory for generated conf files for the UI");
    }

    this.cConfJsonFile = new File(confDir, JSON_PATH);
    this.sConfJsonFile = new File(confDir, JSON_SECURITY_PATH);

    generateConfigFile(cConfJsonFile, cConf);
    generateConfigFile(sConfJsonFile, sConf);

    File uiPath = new File("cdap-ui", "server.js");
    if (!uiPath.exists()) {
      uiPath = new File("ui", "server.js");
    }
    Preconditions.checkState(uiPath.exists(), "Missing server.js at " + uiPath.getAbsolutePath());
    ProcessBuilder builder = new ProcessBuilder(NODE_JS_EXECUTABLE,
                                                uiPath.getAbsolutePath(),
                                                "cConf=\"" + cConfJsonFile.getAbsolutePath() + "\"",
                                                "sConf=\"" + sConfJsonFile.getAbsolutePath() + "\"");
    builder.redirectErrorStream(true);
    LOG.info("Starting UI...");
    process = builder.start();
    final InputStream is = process.getInputStream();
    final InputStreamReader isr = new InputStreamReader(is);
    bufferedReader = new BufferedReader(isr);
  }

  private void generateConfigFile(File path, Configuration config) throws Exception {
    try (Writer configWriter = Files.newWriter(path, Charsets.UTF_8)) {
      ConfigurationJsonTool.exportToJson(config, configWriter);
    }
  }

  /**
   * Processes the output of the command.
   */
  @Override
  protected void run() throws Exception {
    LOG.info("UI running ...");
    try {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        LOG.trace(line);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Returns the {@link Executor} that will be used to run this service.
   */
  @Override
  protected Executor executor() {
    final String name = getClass().getSimpleName();
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread thread = new Thread(command, name);
        thread.setDaemon(true);
        thread.start();
      }
    };
  }

  /**
   * Invoked to request the service to stop.
   * <p/>
   * <p>By default this method does nothing.
   */
  @Override
  protected void triggerShutdown() {
    process.destroy();
  }

  /**
   * Stop the service.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down UI ...");
    process.waitFor();

    // Cleanup generated files
    if (cConfJsonFile != null) {
      cConfJsonFile.delete();
    }
    if (sConfJsonFile != null) {
      sConfJsonFile.delete();
    }
  }
}
