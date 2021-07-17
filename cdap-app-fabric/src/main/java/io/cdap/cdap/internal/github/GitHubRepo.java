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

public class GitHubRepo {
  private String nickname;
  private String url;
  private String defaultBranch;
  private String authMethod;
  private String authToken;
  private String username;
  private String password;

  public GitHubRepo(String nickname, String url, String defaultBranch,
      String authMethod, String username, String password, String authToken) {
    this.nickname = nickname;
    this.url = url;
    this.defaultBranch = defaultBranch;
    this.authMethod = authMethod;
    this.username = username;
    this.password = password;
    this.authToken = authToken;
  }

  public String getNickname() {
    return this.nickname;
  }

  public String getUrl() {
    return this.url;
  }

  public String getDefaultBranch() {
    return this.defaultBranch;
  }

  public String getAuthMethod() {
    return this.authMethod;
  }

  public String getAuthToken() {
    return this.authToken;
  }

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.password;
  }

  public String toString() {
    return "{\n" +
        "  nickname: " + this.nickname + " {\n" +
        "              url: " + this.url + "\n" +
        "              default_branch: " + this.defaultBranch + "\n" +
        "              auth_method: " + this.authMethod + "\n" +
        "              username: " + this.url + "\n" +
        "              password: " + this.url + "\n" +
        "              authorization token: " + this.url + "\n" + "   }" +
        "\n}";
  }
}

