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

/**
 * Auth Configuration for the linked repository.
 */
public class AuthConfig {

  private final AuthType type;
  private final PatConfig patConfig;
  
  /**
   * Constructor for AuthConfig.

   * @param authType {@link AuthType} to use
   * @param patConfig {@link PatConfig} the PAT config
   */
  public AuthConfig(AuthType authType, PatConfig patConfig) {
    this.type = authType;
    this.patConfig = patConfig;
  }

  public AuthType getType() {
    return type;
  }

  public PatConfig getPatConfig() {
    return patConfig;
  }

  /**
   * Validate the AuthConfig.

   * @return a collection of {@link RepositoryValidationFailure}.
   */
  public Collection<RepositoryValidationFailure> validate(Provider provider) {
    Collection<RepositoryValidationFailure> failures = new ArrayList<>();
    if (type == null) {
      failures.add(new RepositoryValidationFailure("'type' must be specified in 'auth'."));
    }

    if (type == AuthType.PAT) {
      if (patConfig == null) {
        failures.add(new RepositoryValidationFailure("'patConfig' must be specified in 'auth'."));
      } else {
        failures.addAll(patConfig.validate(provider));
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
    AuthConfig that = (AuthConfig) o;
    return Objects.equals(type, that.type)
        && Objects.equals(patConfig, that.patConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, patConfig);
  }

  @Override
  public String toString() {
    return "Auth{"
        + "type=" + type
        + ", patConfig=" + patConfig
        + '}';
  }
}
