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

package io.cdap.cdap.internal.app.sourcecontrol;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.SourceControlConfig;

import javax.inject.Inject;

/**
 * The default SourceControlOperationRunnerFactory that creates the InMemorySourceControlOperationRunner.
 */
public final class DefaultSourceControlOperationRunnerFactory implements SourceControlOperationRunnerFactory {

  private final CConfiguration cConf;
  private final SecureStore secureStore;

  @Inject
  public DefaultSourceControlOperationRunnerFactory(CConfiguration cConf,
                                                    SecureStore secureStore) {
    this.cConf = cConf;
    this.secureStore = secureStore;
  }

  @Override
  public SourceControlOperationRunner create(NamespaceId namespace, RepositoryConfig repositoryConfig)
    throws Exception {
    // TODO: CDAP-20322 add the remoteSourceControlOperationRunner
    
    SourceControlConfig config = new SourceControlConfig(namespace, repositoryConfig, cConf);
    try (RepositoryManager repoManager = new RepositoryManager(secureStore, config)) {
      return new InMemorySourceControlOperationRunner(repoManager);
    }
  }
}
