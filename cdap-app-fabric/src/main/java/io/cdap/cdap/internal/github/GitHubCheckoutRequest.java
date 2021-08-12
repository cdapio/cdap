/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.github;

/**
 * Handles the request to check out pipelines from GitHub to CDAP
 */
public class GitHubCheckoutRequest {
  String branch;
  String path;
  public GitHubCheckoutRequest(String branch, String path) {
    this.branch = branch;
    this.path = path;
  }
  public String getBranch() {
    return this.branch;
  }
  public String getPath() {
    return this.path;
  }
}
