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

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Dashboard Store Management.
 */
public class DashboardStore {
  private static final Logger LOG = LoggerFactory.getLogger(DashboardStore.class);
  private static final String CONFIG_TYPE = "dashboard";

  private final ConfigStore configStore;

  @Inject
  public DashboardStore(ConfigStore configStore) {
    this.configStore = configStore;
  }

  public String create(String namespace, Map<String, String> props) throws ConfigExistsException {
    String dashboardId = UUID.randomUUID().toString();
    configStore.create(namespace, CONFIG_TYPE, new Config(dashboardId, props));
    return dashboardId;
  }

  public void delete(String namespace, String id) throws ConfigNotFoundException {
    configStore.delete(namespace, CONFIG_TYPE, id);
  }

  public List<Config> list(String namespace) {
    return configStore.list(namespace, CONFIG_TYPE);
  }

  public Config get(String namespace, String id) throws ConfigNotFoundException {
    return configStore.get(namespace, CONFIG_TYPE, id);
  }

  public void put(String namespace, Config config) throws ConfigNotFoundException {
    configStore.update(namespace, CONFIG_TYPE, config);
  }

  public void delete(String namespace) {
    List<Config> configList = configStore.list(namespace, CONFIG_TYPE);
    for (Config config : configList) {
      try {
        configStore.delete(namespace, CONFIG_TYPE, config.getId());
      } catch (ConfigNotFoundException e) {
        LOG.warn("Tried to delete Dashboard {} in namespace {}, but it was not found", config.getId(), namespace);
      }
    }
  }
}
