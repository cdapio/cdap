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

/**
 * Metadata for {@link CredentialProvisionerProfile}.
 */
public class CredentialProvisionerProfileMetadata {

  private final String provisionerType;
  private final String description;
  private final Map<String, String> properties;

  public CredentialProvisionerProfileMetadata(String provisionerType, String description,
      Map<String, String> properties) {
    this.provisionerType = provisionerType;
    this.description = description;
    this.properties = Collections.unmodifiableMap(properties);
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
}
