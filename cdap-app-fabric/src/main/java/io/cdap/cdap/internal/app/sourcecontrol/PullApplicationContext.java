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

import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

/**
 * Information required by {@link SourceControlOperationRunner}
 * to pull application specification from linked repository.
 */
public class PullApplicationContext {
  private final String applicationName;
  private final String branchName;
  private final RepositoryConfig repositoryConfig;

  public PullApplicationContext(String applicationName, String branchName, RepositoryConfig repositoryConfig) {
    this.applicationName = applicationName;
    this.branchName = branchName;
    this.repositoryConfig = repositoryConfig;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public RepositoryConfig getRepositoryConfig() {
    return repositoryConfig;
  }

  public String getBranchName() {
    return branchName;
  }
}
