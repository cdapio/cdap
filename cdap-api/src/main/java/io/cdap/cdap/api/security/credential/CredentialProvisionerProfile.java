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

import com.google.gson.Gson;
import java.util.Collections;
import java.util.Map;

/**
 * Defines a profile for credential provisioning.
 */
public class CredentialProvisionerProfile implements Secured<CredentialProvisionerProfileMetadata> {

  private static final Gson GSON = new Gson();

  private final CredentialProvisionerProfileMetadata metadata;
  private final Map<String, String> secureProperties;

  public CredentialProvisionerProfile(CredentialProvisionerProfileMetadata metadata,
      Map<String, String> secureProperties) {
    this.metadata = metadata;
    this.secureProperties = Collections.unmodifiableMap(secureProperties);
  }

  @Override
  public CredentialProvisionerProfileMetadata getMetadata() {
    return metadata;
  }

  public Map<String, String> getSecureProperties() {
    return secureProperties;
  }

  @Override
  public String getSecureData() {
    return GSON.toJson(secureProperties);
  }
}
