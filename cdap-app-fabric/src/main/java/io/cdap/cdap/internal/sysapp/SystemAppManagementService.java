/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.sysapp;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Enables system apps using system app config files located in SYSTEM_APP_CONFIG_DIR. Each config
 * describes steps to perform to start one (or more) system app. Currently
 * SystemAppManagementService runs during bootstrap phase.
 */
public class SystemAppManagementService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(SystemAppManagementService.class);
  private static final Gson GSON = new Gson();

  private final File systemAppConfigDirPath;
  private final SystemAppEnableExecutor systemAppEnableExecutor;
  private ExecutorService executorService;

  @Inject
  SystemAppManagementService(CConfiguration cConf, ApplicationLifecycleService appLifecycleService,
                             ProgramLifecycleService programLifecycleService) {
    this.systemAppConfigDirPath = new File(cConf.get(Constants.SYSTEM_APP_CONFIG_DIR));
    this.systemAppEnableExecutor = new SystemAppEnableExecutor(appLifecycleService, programLifecycleService);
  }

  @Override
  protected void startUp() {
    LOG.debug("Starting {}", getClass().getSimpleName());
    executorService =
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("sys-app-management-service"));
    executorService.execute(() -> {
      try {
        // Execute all steps for each config file in system config directory.
        bootStrapSystemAppConfigDir();
      } catch (Exception ex) {
        LOG.error("Got exception in watch service for system app config dir", ex);
      }
    });
  }

  private void bootStrapSystemAppConfigDir() throws Exception {
    LOG.debug("Number of config files {} in system app config directory.", systemAppConfigDirPath.listFiles().length);
    for (File sysAppConfigFile : DirUtils.listFiles(systemAppConfigDirPath)) {
      LOG.debug("Running steps in config file {}", sysAppConfigFile.getAbsoluteFile());
      executeSysConfig(sysAppConfigFile.getAbsoluteFile());
    }
  }

  private void executeSysConfig(File fileName) throws Exception {
    SystemAppConfig config = parseConfig(fileName);
    for (SystemAppStep step : config.getSteps()) {
      try {
        step.validate();
      } catch (IllegalArgumentException e) {
        LOG.warn("Config step {} failed because it is malformed: {}", step.getLabel(), e);
        return;
      }
      if (step.getType() == SystemAppStep.Type.ENABLE_SYSTEM_APP) {
        systemAppEnableExecutor.deployAppAndStartPrograms(step.getArguments());
        LOG.debug("Deployed and enabled system app with id {} and label {}. Config file: {}.",
                  step.getArguments().getId(), step.getLabel(), fileName);
      } else {
        LOG.warn("Unknown system app step type {} for step label {}. Config file: {}. Ignoring it.",
                 step.getLabel(), step.getType(), fileName);
      }
    }
  }

  private SystemAppConfig parseConfig(File fileName) {
    try (Reader reader = new FileReader(fileName)) {
      return GSON.fromJson(reader, SystemAppConfig.class);
    } catch (FileNotFoundException e) {
      LOG.info("System app config file {} does not exist.", fileName);
      return SystemAppConfig.EMPTY;
    } catch (JsonParseException e) {
      LOG.warn("Could not parse system app config file {}.", fileName, e);
      return SystemAppConfig.EMPTY;
    } catch (IOException e) {
      LOG.warn("Could not read system app config file {}.", fileName, e);
      return SystemAppConfig.EMPTY;
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Shutdown the executor, which will issue an interrupt to the running thread.
    // There is only a single daemon thread, so no need to wait for termination
    executorService.shutdownNow();
    LOG.debug("Stopped {}", getClass().getSimpleName());
  }
}
