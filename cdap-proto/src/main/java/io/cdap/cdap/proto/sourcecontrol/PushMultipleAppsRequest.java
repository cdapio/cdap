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

import io.cdap.cdap.proto.id.NamespaceId;
import java.util.List;

/**
 * The request class to push multiple applications (in the same namespace) to linked git repository.
 */
public class PushMultipleAppsRequest {
  private final String commitMessage;
  private final List<String> apps;

  public PushMultipleAppsRequest(List<String> apps, String commitMessage) {
    this.apps = apps;
    this.commitMessage = commitMessage;
  }

  public String getCommitMessage() {
    return commitMessage;
  }

  public List<String> getApps() {
    return apps;
  }
}
