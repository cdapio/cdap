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

package io.cdap.cdap.api.security.credential;

import java.util.Collections;
import java.util.Map;

public class CredentialProvisionerProfile {

  private final String name;
  private final String provisionerType;
  private final String description;
  private final Map<String, String> properties;
  private final Map<String, String> secureProperties;

  public CredentialProvisionerProfile(String name, String provisionerType, String description,
      Map<String, String> properties, Map<String, String> secureProperties) {
    this.name = name;
    this.provisionerType = provisionerType;
    this.description = description;
    this.properties = Collections.unmodifiableMap(properties);
    this.secureProperties = Collections.unmodifiableMap(secureProperties);
  }

  public String getName() {
    return name;
  }

  public String getProvisionerType() {
    return provisionerType;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Map<String, String> getSecureProperties() {
    return secureProperties;
  }
}
