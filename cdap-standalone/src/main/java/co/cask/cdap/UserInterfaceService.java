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
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.concurrent.Executor;

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

  static final String UI;
  static {
    // Determine what's the path to the server.js, based on what's on the directory
    // When run from IDE, the base is "cdap-ui" and from standalone it's ui.
    File base = new File("ui");
    if (!base.isDirectory()) {
      base = new File("cdap-ui");
    }
    UI = new File(base, "server.js").getAbsolutePath();
  }

  private final File uiBase;
  private final File uiPath;
  private final CConfiguration cConf;
  private final SConfiguration sConf;

  private Process process;
  private BufferedReader bufferedReader;

  @Inject
  public UserInterfaceService(@Named("ui-path") String uiPath, CConfiguration cConf, SConfiguration sConf) {
    this.uiPath = new File(uiPath);
    if (!uiPath.isEmpty()) {
      this.uiBase = this.uiPath.getParentFile().getParentFile().getParentFile();
    } else {
      this.uiBase = null;
      LOG.warn("UI Path is empty");
    }
    // This is ok since this class is only used in standalone, the path is always [base]/server/local/main.js
    // However, this could change if the layer of ui changed, which require adjustment to this class anyway
    this.cConf = cConf;
    this.sConf = sConf;
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {
    File confDir = new File(new File(uiBase, "conf"), "generated");
    if (!confDir.exists()) {
      Preconditions.checkState(confDir.mkdirs(), "Couldn't create directory for generated conf files for the UI");
    }
    generateConfigFile(new File(confDir, JSON_PATH), cConf);
    generateConfigFile(new File(confDir, JSON_SECURITY_PATH), sConf);

    ProcessBuilder builder = new ProcessBuilder(NODE_JS_EXECUTABLE, uiPath.getAbsolutePath());
    builder.redirectErrorStream(true);
    LOG.info("Starting UI ... (" + uiPath + ")");
    process = builder.start();
    final InputStream is = process.getInputStream();
    final InputStreamReader isr = new InputStreamReader(is);
    bufferedReader = new BufferedReader(isr);
  }

  private void generateConfigFile(File path, Configuration config) throws Exception {
    Writer configWriter = Files.newWriter(path, Charsets.UTF_8);
    try {
      ConfigurationJsonTool.exportToJson(config, configWriter);
    } finally {
      configWriter.close();
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
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread thread = new Thread(command, getServiceName());
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
    new File(uiBase, JSON_PATH).delete();
    new File(uiBase, JSON_SECURITY_PATH).delete();
  }
}
