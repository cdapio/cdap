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

package io.cdap.cdap.proto.sourcecontrol;

import io.cdap.cdap.proto.DeploymentDryrunResult;

public class PullAppDryrunResponse {
  private final String applicationName;
  private final String applicationFileHash;
  private final DeploymentDryrunResult result;

  public PullAppDryrunResponse(String applicationName, String applicationFileHash, DeploymentDryrunResult result) {
    this.applicationName = applicationName;
    this.applicationFileHash = applicationFileHash;
    this.result = result;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getApplicationFileHash() {
    return applicationFileHash;
  }

  public DeploymentDryrunResult getResult() { return result; }
}
