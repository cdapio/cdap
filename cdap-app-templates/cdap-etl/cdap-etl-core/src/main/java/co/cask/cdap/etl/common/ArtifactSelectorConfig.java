/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import com.google.common.base.CharMatcher;

/**
 * Part of the etl configuration, used to choose which artifact to use for a plugin. Normally created through
 * deserialization by the CDAP framework. Programmatic creation is only used for unit tests.
 */
public class ArtifactSelectorConfig {
  private static final CharMatcher nameMatcher =
    CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.inRange('0', '9'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.is('-'));
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

  /**
   * Gets the corresponding {@link ArtifactSelector} for this config.
   * Validates that any given scope, name, and version are all valid or null. The scope must be an
   * {@link ArtifactScope}, the version must be an {@link ArtifactVersion}, and the name only contains
   * alphanumeric, '-', or '_'. Also checks that at least one field is non-null.
   *
   * @return an {@link ArtifactSelector} using these config settings
   * @throws IllegalArgumentException if any one of the fields are invalid
   */
  public ArtifactSelector getArtifactSelector(String pluginType, String pluginName) {
    if (name != null && !nameMatcher.matchesAllOf(name)) {
      throw new IllegalArgumentException(String.format("'%s' is an invalid artifact name. " +
                                                         "Must contain only alphanumeric, '-', or '_' characters.",
                                                       name));
    }

    ArtifactVersion artifactVersion = null;
    if (version != null) {
      artifactVersion = new ArtifactVersion(version);
      if (artifactVersion.getVersion() == null) {
        throw new IllegalArgumentException(String.format("Could not parse '%s' as an artifact version.", version));
      }
    }

    ArtifactScope artifactScope = scope == null ? null : ArtifactScope.valueOf(scope.toUpperCase());
    return new ArtifactSelector(pluginType, pluginName, artifactScope, name, artifactVersion);
  }
}
