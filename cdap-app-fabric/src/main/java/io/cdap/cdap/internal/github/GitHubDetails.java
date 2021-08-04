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

public class GitHubDetails {
  String title;
  String body;
  String head;
  String base;
  String content;
  String sha;
  JsonObject object;
  public GitHubDetails (String title, String body, String head, String base,
      String content, String sha, JsonObject object) {
    this.title = title;
    this.body = body;
    this.head = head;
    this.base = base;
    this.content = content;
    this.sha = sha;
    this.object = object;
  }

  public JsonObject getObject() {
    return object;
  }

  public String getBase() {
    return base;
  }

  public String getBody() {
    return body;
  }

  public String getContent() {
    return content;
  }

  public String getHead() {
    return head;
  }

  public String getSha() {
    return sha;
  }

  public String getTitle() {
    return title;
  }
}
