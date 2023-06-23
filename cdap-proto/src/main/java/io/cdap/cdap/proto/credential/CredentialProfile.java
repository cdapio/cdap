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

package io.cdap.cdap.proto.credential;

import java.util.Map;

/**
 * Defines a profile for credential provisioning.
 */
public class CredentialProfile {

  private final String credentialProviderType;
  private final String description;
  private final Map<String, String> properties;

  public CredentialProfile(String credentialProviderType, String description,
      Map<String, String> properties) {
    this.credentialProviderType = credentialProviderType;
    this.description = description;
    this.properties = properties;
  }

  public String getCredentialProviderType() {
    return credentialProviderType;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
