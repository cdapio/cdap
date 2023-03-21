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

  @Nullable
  public String getUsername() {
    return username;
  }

  /**
   * Validate PatConfig.

   * @return a collection of {@link RepositoryValidationFailure}.
   */
  public Collection<RepositoryValidationFailure> validate() {
    Collection<RepositoryValidationFailure> failures = new ArrayList<>();
    if (passwordName == null || passwordName.equals("")) {
      failures.add(new RepositoryValidationFailure("'passwordName' must be specified in 'patConfig'."));
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
