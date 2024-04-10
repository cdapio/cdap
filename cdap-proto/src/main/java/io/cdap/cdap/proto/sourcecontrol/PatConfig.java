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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The PAT configuration associated with the source control management.
 */
public class PatConfig {
  private final String passwordName;
  private final String username;

  private static final String BITBUCKET_USERNAME = "x-token-auth";

  /**
   * Construct a PAT config.
   *
   * @param passwordName the password name
   * @param username the username
   */
  public PatConfig(String passwordName, @Nullable String username) {
    this.passwordName = passwordName;
    this.username = username;
  }

  public String getPasswordName() {
    return passwordName;
  }

  /**
   * Returns the username to use for authentication.
   */
  public String getUsername() {
    // If username is not specified, use X_TOKEN_AUTH as username as for Bitbucket it should be
    // BITBUCKET_USERNAME and GitHub/gitlab it can be any string.
    return username == null || username.isEmpty() ? BITBUCKET_USERNAME : username;
  }

  /**
   * Validate PatConfig.
   *
   * @return a collection of {@link RepositoryValidationFailure}.
   */
  public Collection<RepositoryValidationFailure> validate(Provider provider) {
    Collection<RepositoryValidationFailure> failures = new ArrayList<>();
    if (passwordName == null || passwordName.equals("")) {
      failures.add(
          new RepositoryValidationFailure("'passwordName' must be specified in 'patConfig'."));
    }

    if (provider == Provider.BITBUCKET_CLOUD || provider == Provider.BITBUCKET_SERVER) {
      if (!BITBUCKET_USERNAME.equals(getUsername())) {
        failures.add(
            new RepositoryValidationFailure(
                String.format("'username' must be '%s' for Bitbucket", BITBUCKET_USERNAME)));
      }
    }

    return failures;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PatConfig that = (PatConfig) o;
    return Objects.equals(passwordName, that.passwordName)
        && Objects.equals(username, that.username);
  }

  @Override
  public int hashCode() {
    return Objects.hash(passwordName, username);
  }

  @Override
  public String toString() {
    return "PatConfig{"
        + "passwordName=" + passwordName
        + ", username=" + username
        + '}';
  }
}
