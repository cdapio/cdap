/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.NamespaceMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A service for keeping namespace configs in sync when appfabric starts up.
 */
public class NamespaceConfigSyncService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceConfigSyncService.class);
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  NamespaceConfigSyncService(NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  protected void run() throws Exception {
    MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
    if (masterEnv == null) {
      return;
    }

    Map<String, Map<String, String>> namespaceConfigs = namespaceQueryAdmin.list().stream().collect(
      Collectors.toMap(NamespaceMeta::getName, meta -> meta.getConfig().getConfigs()));
    while (true) {
      try {
        masterEnv.syncNamespaceConfigs(namespaceConfigs);
        return;
      } catch (Exception e) {
        LOG.warn("Failed to sync namespace configs", e);
        TimeUnit.MINUTES.sleep(1);
      }
    }
  }
}
