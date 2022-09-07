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

package io.cdap.cdap.internal.capability;

import com.google.gson.JsonObject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import org.json.JSONObject;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class with fields for requesting an Application deployment for a capability
 */
public class SystemApplication {

  private final String namespace;
  private final String name;
  private final String version;
  private final ArtifactSummary artifact;
  private final JsonObject config;

  public SystemApplication(String namespace, String applicationName, @Nullable String version,
                           ArtifactSummary artifact, @Nullable JsonObject config) {
    this.namespace = namespace;
    this.name = applicationName;
    this.version = version;
    this.artifact = artifact;
    this.config = config;
  }

  /**
   * @return namespace {@link String}
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return name {@link String}
   */
  public String getName() {
    return name;
  }

  /**
   * @return version {@link String}, could be null
   */
  @Nullable
  public String getVersion() {
    return version;
  }

  /**
   * @return {@link ArtifactSummary}
   */
  public ArtifactSummary getArtifact() {
    return artifact;
  }

  /**
   * @return {@link JSONObject} configuration
   */
  @Nullable
  public JsonObject getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    SystemApplication otherApplication = (SystemApplication) other;
    return Objects.equals(namespace, otherApplication.namespace) &&
      Objects.equals(name, otherApplication.name) &&
      Objects.equals(version, otherApplication.version) &&
      Objects.equals(artifact, otherApplication.artifact) &&
      Objects.equals(config, otherApplication.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, version, artifact, config);
  }
}
