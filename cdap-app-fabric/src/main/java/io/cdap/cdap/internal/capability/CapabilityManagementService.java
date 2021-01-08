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

package io.cdap.cdap.internal.capability;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.services.SystemProgramManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Scan service for capability configurations
 */
public class CapabilityManagementService extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(CapabilityManagementService.class);
  private static final Gson GSON = new Gson();
  private final long scheduleIntervalInMillis;
  private final CConfiguration cConf;
  private final CapabilityApplier capabilityApplier;
  private final SystemProgramManagementService systemProgramManagementService;

  @Inject
  CapabilityManagementService(CConfiguration cConf, CapabilityApplier capabilityApplier,
                              SystemProgramManagementService systemProgramManagementService) {
    super(RetryStrategies
            .fixDelay(cConf.getLong(Constants.Capability.DIR_SCAN_INTERVAL_MINUTES), TimeUnit.MINUTES));
    this.cConf = cConf;
    this.capabilityApplier = capabilityApplier;
    this.systemProgramManagementService = systemProgramManagementService;
    this.scheduleIntervalInMillis = TimeUnit.MINUTES
      .toMillis(cConf.getLong(Constants.Capability.DIR_SCAN_INTERVAL_MINUTES));
  }

  @Override
  protected void doStartUp() {
    LOG.debug("Starting scheduled service {}", getServiceName());
    systemProgramManagementService.start();
  }

  @Override
  protected void doShutdown() {
    LOG.debug("Stopping scheduled service {}", getServiceName());
    systemProgramManagementService.stopAndWait();
  }

  @Override
  protected long runTask() throws Exception {
    LOG.trace("Scanning capability config directory");
    List<CapabilityConfig> capabilityConfigs = scanConfigDirectory();
    //apply all the config
    capabilityApplier.apply(capabilityConfigs);
    return scheduleIntervalInMillis;
  }

  private List<CapabilityConfig> scanConfigDirectory() {
    File configDir = new File(cConf.get(Constants.Capability.CONFIG_DIR));
    List<CapabilityConfig> capabilityConfigs = new ArrayList<>();
    for (File configFile : DirUtils.listFiles(configDir)) {
      if (!configFile.isFile()) {
        continue;
      }
      try (Reader reader = new FileReader(configFile)) {
        CapabilityConfig capabilityConfig = GSON.fromJson(reader, CapabilityConfig.class);
        capabilityConfigs.add(capabilityConfig);
      } catch (Exception exception) {
        LOG.debug("Exception reading capability config file {}", configFile, exception);
      }
    }
    return capabilityConfigs;
  }
}
