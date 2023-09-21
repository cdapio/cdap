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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PushMultipleAppsRequest {
  private final List<String> appIds;
  private final String commitMessage;

  public PushMultipleAppsRequest(List<String> appIds, String commitMessage) {
    this.appIds = appIds;
    this.commitMessage = commitMessage;
  }

  public List<String> getAppIds() {
    if (null == appIds) {
      return new ArrayList<>();
    }
    return appIds;
  }

  public String getCommitMessage() {
    return commitMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PushMultipleAppsRequest)) {
      return false;
    }
    PushMultipleAppsRequest that = (PushMultipleAppsRequest) o;
    return this.commitMessage.equals(that.commitMessage)
        && Objects.equals(appIds, that.appIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appIds, commitMessage);
  }
}
