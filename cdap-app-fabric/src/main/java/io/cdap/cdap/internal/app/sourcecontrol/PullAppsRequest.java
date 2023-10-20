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
import java.util.Set;

/**
 * Request type for {@link PullAppsOperation}.
 */
public class PullAppsRequest {

  private final Set<String> apps;

  /**
   * Default Constructor.
   *
   * @param apps Set of apps to pull.
   */
  public PullAppsRequest(Set<String> apps) {
    this.apps = apps;
  }

  public Set<String> getApps() {
    return apps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PullAppsRequest that = (PullAppsRequest) o;
    return Objects.equal(this.getApps(), that.getApps());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getApps());
  }
}
