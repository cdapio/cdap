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

import com.google.gson.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class GitHubIO {
  String branch;
  String path;
  String message;
  JsonObject content;
  public GitHubIO(String branch, String path, String message, JsonObject content) {
    this.branch = branch;
    this.path = path;
    this.message = message;
    this.content = content;
  }

  public String getBranch() {
    return this.branch;
  }
  public String getPath() {
    return this.path;
  }
  public String getCommitMessage() {
    return this.message;
  }
  public JsonObject getPipeline() {
    return this.content;
  }
  public String getEncodedPipeline() {
    return Base64.getEncoder().encodeToString(content.toString().getBytes(
        StandardCharsets.UTF_8));
  }
}
