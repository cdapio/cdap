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

package co.cask.cdap.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Console Settings Store Management.
 */
public class ConsoleSettingsStore {
  private static final Logger LOG = LoggerFactory.getLogger(ConsoleSettingsStore.class);
  private static final String NAMESPACE = "";
  private static final String CONFIG_TYPE = "usersettings";

  private final ConfigStore configStore;

  @Inject
  public ConsoleSettingsStore(ConfigStore configStore) {
    this.configStore = configStore;
  }

  public Config get(String userId) throws ConfigNotFoundException {
    return configStore.get(NAMESPACE, CONFIG_TYPE, userId);
  }

  public void delete(String userId) throws ConfigNotFoundException {
    configStore.delete(NAMESPACE, CONFIG_TYPE, userId);
  }

  public void put(Config userConfig) {
    configStore.createOrUpdate(NAMESPACE, CONFIG_TYPE, userConfig);
  }

  @VisibleForTesting
  List<Config> list() {
    return configStore.list(NAMESPACE, CONFIG_TYPE);
  }

  public void delete() {
    List<Config> configList = configStore.list(NAMESPACE, CONFIG_TYPE);
    for (Config config : configList) {
      try {
        configStore.delete(NAMESPACE, CONFIG_TYPE, config.getId());
      } catch (ConfigNotFoundException e) {
        LOG.warn("ConsoleSettings for {} not found", config.getId());
      }
    }
  }
}
