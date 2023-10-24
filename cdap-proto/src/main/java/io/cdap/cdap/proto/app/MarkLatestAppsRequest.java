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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Request class for the markLatest api.
 */
public class MarkLatestAppsRequest {
  private final List<AppVersion> apps;

  public MarkLatestAppsRequest(List<AppVersion> apps) {
    this.apps = apps;
  }

  /**
   * Get the list of apps the are to be marked latest.
   *
   * @return List of {@link AppVersion} or an empty list. Should not return null.
   */
  public List<AppVersion> getApps() {
    if (null == apps) {
      return new ArrayList<>();
    }
    return apps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MarkLatestAppsRequest)) {
      return false;
    }
    MarkLatestAppsRequest that = (MarkLatestAppsRequest) o;
    return Objects.equals(apps, that.apps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apps);
  }
}
