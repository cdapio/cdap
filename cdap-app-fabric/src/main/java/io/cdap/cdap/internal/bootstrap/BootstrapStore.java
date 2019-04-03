/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.config.Config;
import io.cdap.cdap.config.ConfigNotFoundException;
import io.cdap.cdap.config.ConfigStore;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.Collections;

/**
 * Fetches and stores bootstrap state. This simply delegates to the ConfigStore in order to avoid have a table
 * that has just a single row in it.
 */
public class BootstrapStore {
  private static final String TYPE = "bootstrap";
  private static final String NAME = "state";
  private final ConfigStore configStore;

  @Inject
  BootstrapStore(ConfigStore configStore) {
    this.configStore = configStore;
  }

  /**
   * @return whether the CDAP instance is bootstrapped.
   */
  public boolean isBootstrapped() {
    try {
      configStore.get(NamespaceId.SYSTEM.getNamespace(), TYPE, NAME);
      return true;
    } catch (ConfigNotFoundException e) {
      return false;
    }
  }

  /**
   * Mark the CDAP instance as bootstrapped.
   */
  public void bootstrapped() {
    configStore.createOrUpdate(NamespaceId.SYSTEM.getNamespace(), TYPE,
                               new Config(NAME, Collections.emptyMap()));
  }

  /**
   * Clear bootstrap state. This should only be called in tests.
   */
  @VisibleForTesting
  void clear() {
    try {
      configStore.delete(NamespaceId.SYSTEM.getNamespace(), TYPE, NAME);
    } catch (ConfigNotFoundException e) {
      // does not matter, ignore it
    }
  }

}
