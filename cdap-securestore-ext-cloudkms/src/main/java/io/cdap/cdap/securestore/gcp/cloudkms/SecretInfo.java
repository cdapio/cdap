/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.securestore.gcp.cloudkms;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Class used to serialize and deserialize the secret and associated metadata. Used by {@link SecretInfoCodec}.
 */
public final class SecretInfo {
  private final String name;
  private final String description;
  private final byte[] secretData;
  private final long creationTimeMs;
  private final Map<String, String> properties;

  /**
   * Creates SecretInfo instance with provided name, description, secretData, creationTimeMs and properties.
   */
  public SecretInfo(String name, @Nullable String description, byte[] secretData, long creationTimeMs,
                    Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.secretData = secretData;
    this.creationTimeMs = creationTimeMs;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  /**
   * Returns name of the secret
   */
  public String getName() {
    return name;
  }

  /**
   * Returns description of the secret
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * Returns secretData of the secret
   */
  public byte[] getSecretData() {
    return secretData;
  }

  /**
   * Returns creationTimeMs of the secret
   */
  public long getCreationTimeMs() {
    return creationTimeMs;
  }

  /**
   * Returns properties of the secret
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
