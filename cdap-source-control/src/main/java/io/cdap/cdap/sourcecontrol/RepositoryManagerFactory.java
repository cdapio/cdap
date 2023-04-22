/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

/**
 * The default implementation for RepositoryManagerFactory
 */
public class RepositoryManagerFactory {

  private final SecureStore secureStore;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  RepositoryManagerFactory(SecureStore secureStore, CConfiguration cConf,
      MetricsCollectionService metricsCollectionService) {
    this.secureStore = secureStore;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
  }

  public RepositoryManager create(NamespaceId namespace, RepositoryConfig repoConfig) {
    return new RepositoryManager(secureStore, cConf, namespace, repoConfig,
        metricsCollectionService);
  }
}
