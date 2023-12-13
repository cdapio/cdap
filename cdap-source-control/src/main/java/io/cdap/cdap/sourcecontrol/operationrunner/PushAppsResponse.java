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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Encapsulates the information generated from push operation.
 */
public class PushAppsResponse {

  private final Collection<PushAppMeta> apps;
  private final String commitId;

  public PushAppsResponse(Collection<PushAppMeta> apps, String commitId) {
    this.apps = Collections.unmodifiableCollection(apps);
    this.commitId = commitId;
  }

  public Collection<PushAppMeta> getApps() {
    return apps;
  }

  public String getCommitId() {
    return commitId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PushAppsResponse that = (PushAppsResponse) o;
    return Objects.equals(apps, that.apps)
        && Objects.equals(commitId, that.commitId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apps, commitId);
  }
}
