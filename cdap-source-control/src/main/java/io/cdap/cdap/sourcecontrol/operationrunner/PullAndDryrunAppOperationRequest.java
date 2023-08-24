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

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

/**
 * Information required by {@link SourceControlOperationRunner} to execute the task of
 * pulling an application from linked repository.
 */
public class PullAndDryrunAppOperationRequest {
  private final RepositoryConfig repoConfig;

  private final ApplicationReference app;
  private final ApplicationId appId;

  public PullAndDryrunAppOperationRequest(ApplicationId appId, ApplicationReference app, RepositoryConfig repoConfig) {
    this.appId = appId;
    this.repoConfig = repoConfig;
    this.app = app;
  }

  public RepositoryConfig getRepositoryConfig() {
    return repoConfig;
  }

  public ApplicationReference getApp() {
    return app;
  }

  public ApplicationId getAppId() {
    return appId;
  }
}
