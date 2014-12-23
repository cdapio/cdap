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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserSettings ConfigService.
 */
public class UserSettingsConfigService extends DatasetConfigService {
  private static final Logger LOG = LoggerFactory.getLogger(UserSettingsConfigService.class);

  @Inject
  public UserSettingsConfigService(CConfiguration cConf, DatasetFramework dsFramework, TransactionExecutorFactory
    executorFactory) {
    super(cConf, dsFramework, executorFactory);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting UserSettingsConfigService...");
    super.startUp();
    LOG.info("UserSettingsConfigService started...");
  }

  @Override
  protected String getRowKeyString(String namespace, ConfigType type, String name) {
    return String.format("user.%s", name);
  }
}
