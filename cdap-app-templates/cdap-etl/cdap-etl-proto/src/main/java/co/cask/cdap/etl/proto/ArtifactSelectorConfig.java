/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.proto;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;

import java.util.Objects;

/**
 * Part of the etl configuration, used to choose which artifact to use for a plugin. Normally created through
 * deserialization by the CDAP framework. Programmatic creation is only used for unit tests.
 */
public class ArtifactSelectorConfig {
  private final String scope;
  private final String name;
  private final String version;

  public ArtifactSelectorConfig() {
    this.scope = null;
    this.name = null;
    this.version = null;
  }

  // for unit tests
  public ArtifactSelectorConfig(String scope, String name, String version) {
    this.scope = scope;
    this.name = name;
    this.version = version;
  }

  public String getScope() {
    return scope;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactSelectorConfig that = (ArtifactSelectorConfig) o;

    return Objects.equals(scope, that.scope) &&
      Objects.equals(name, that.name) &&
      Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, name, version);
  }

  @Override
  public String toString() {
    return "ArtifactSelectorConfig{" +
      "scope='" + scope + '\'' +
      ", name='" + name + '\'' +
      ", version='" + version + '\'' +
      '}';
  }
}
