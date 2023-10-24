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

package io.cdap.cdap.proto.app;

import java.util.Objects;

/**
 * Request class to update sourceControlMeta of an application.
 */
public class UpdateSourceControlMetaRequest {
  private final String name;
  private final String appVersion;
  private final String gitFileHash;

  public UpdateSourceControlMetaRequest(String name, String appVersion, String gitFileHash) {
    this.name = name;
    this.appVersion = appVersion;
    this.gitFileHash = gitFileHash;
  }

  public String getName() {
    return name;
  }

  public String getAppVersion() {
    return appVersion;
  }

  public String getGitFileHash() {
    return gitFileHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UpdateSourceControlMetaRequest)) {
      return false;
    }
    UpdateSourceControlMetaRequest that = (UpdateSourceControlMetaRequest) o;
    return Objects.equals(name, that.name) && Objects.equals(appVersion,
        that.appVersion) && Objects.equals(gitFileHash, that.gitFileHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, appVersion, gitFileHash);
  }
}
