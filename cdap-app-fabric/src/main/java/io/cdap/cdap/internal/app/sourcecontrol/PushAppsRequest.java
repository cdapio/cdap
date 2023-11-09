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

import com.google.common.base.Objects;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import java.util.Set;

/**
 * Request type for {@link PullAppsOperation}.
 */
public class PushAppsRequest {

  private final Set<String> apps;
  private final RepositoryConfig config;

  private final CommitMeta commitDetails;

  /**
   * Default Constructor.
   *
   * @param apps Set of apps to push.
   */
  public PushAppsRequest(Set<String> apps, RepositoryConfig config, CommitMeta commitDetails) {
    this.apps = apps;
    this.config = config;
    this.commitDetails = commitDetails;
  }

  public Set<String> getApps() {
    return apps;
  }

  public RepositoryConfig getConfig() {
    return config;
  }

  public CommitMeta getCommitDetails() {
    return commitDetails;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PushAppsRequest that = (PushAppsRequest) o;
    return Objects.equal(this.getApps(), that.getApps())
        && Objects.equal(this.getConfig(), that.getConfig())
        && Objects.equal(this.getCommitDetails(), that.getCommitDetails());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getApps(), getConfig(), getCommitDetails());
  }
}
