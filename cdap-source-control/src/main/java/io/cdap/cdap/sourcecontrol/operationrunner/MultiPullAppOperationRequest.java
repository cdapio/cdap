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

package io.cdap.cdap.sourcecontrol.operationrunner;

import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import java.util.Set;

/**
 * Information required by {@link SourceControlOperationRunner} to execute the task of pulling
 * multiple application from linked repository.
 */
public class MultiPullAppOperationRequest {

  private final RepositoryConfig repoConfig;

  private final NamespaceId namespace;

  private final Set<String> apps;

  /**
   * Default constructor.
   */
  public MultiPullAppOperationRequest(RepositoryConfig repoConfig, NamespaceId namespace,
      Set<String> apps) {
    this.repoConfig = repoConfig;
    this.namespace = namespace;
    this.apps = apps;
  }

  public NamespaceId getNamespace() {
    return namespace;
  }

  public RepositoryConfig getRepositoryConfig() {
    return repoConfig;
  }

  public Set<String> getApps() {
    return apps;
  }

}
