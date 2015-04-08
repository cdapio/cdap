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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.config.Config;
import co.cask.cdap.config.ConfigNotFoundException;
import co.cask.cdap.config.ConfigStore;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store for Adapter related information.
 */
public class AdapterStore {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterStore.class);
  private static final String CONFIG_TYPE = "adapter";
  private static final String RUN_ID = "runId";

  private final ConfigStore configStore;

  @Inject
  public AdapterStore(ConfigStore configStore) {
    this.configStore = configStore;
  }

  public void setRunId(Id.Adapter adapter, RunId runId) {
    Config adapterConfig = new Config(adapter.getId(), ImmutableMap.of(RUN_ID, runId.getId()));
    configStore.createOrUpdate(adapter.getNamespaceId(), CONFIG_TYPE, adapterConfig);
  }

  public RunId getRunId(Id.Adapter adapter) {
    try {
      final Config adapterConfig = configStore.get(adapter.getNamespaceId(), CONFIG_TYPE, adapter.getId());
      return new RunId() {
        @Override
        public String getId() {
          return adapterConfig.getProperties().get(RUN_ID);
        }
      };
    } catch (ConfigNotFoundException e) {
      return null;
    }
  }

  public void deleteRunId(Id.Adapter adapter) {
    try {
      configStore.delete(adapter.getNamespaceId(), CONFIG_TYPE, adapter.getId());
    } catch (ConfigNotFoundException e) {
      LOG.warn("Trying to delete Adapter RunId which is not preset : {}", adapter, e);
    }
  }
}
