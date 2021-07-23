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

import org.elasticsearch.common.Strings;

public class GitHubRepo {
  private String nickname;
  private String url;
  private String defaultBranch;
  private String authString;

  public GitHubRepo(String nickname, String url, String defaultBranch,
      String authString) {
    this.nickname = nickname;
    this.url = url;
    this.defaultBranch = defaultBranch;
    this.authString = authString;
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

  public String getAuthString() {
    return this.authString;
  }

  public boolean validNickname() {
    return !Strings.isNullOrEmpty(this.nickname);
  }

  public boolean validUrl() {
    return !Strings.isNullOrEmpty(this.url);
  }

  public boolean validDefaultBranch() {
    return !Strings.isNullOrEmpty(this.defaultBranch);
  }

  public boolean validAuthString() {
    return !Strings.isNullOrEmpty(this.authString);
  }

  public boolean validateAllFields() {
    return validNickname() && validUrl() && validDefaultBranch() && validAuthString();
  }
  @Override
  public String toString() {
    return "{\n" +
        "  nickname: " + this.nickname + " {\n" +
        "                       url: " + this.url + "\n" +
        "                       default_branch: " + this.defaultBranch + "\n" +
        "                       auth_string: " + this.authString + "\n" + "   }" +
        "\n}";
  }
}

