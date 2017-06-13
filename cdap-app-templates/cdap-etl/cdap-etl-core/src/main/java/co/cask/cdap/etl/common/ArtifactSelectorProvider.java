/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.api.artifact.ArtifactVersionRange;
import co.cask.cdap.api.artifact.InvalidArtifactRangeException;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import com.google.common.base.CharMatcher;

import javax.annotation.Nullable;

/**
 * Provides the {@link ArtifactSelector} from a {@link ArtifactSelectorConfig}.
 */
public class ArtifactSelectorProvider {
  private static final CharMatcher nameMatcher =
    CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.inRange('0', '9'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.is('.'))
      .or(CharMatcher.is('-'));

  private final String pluginType;
  private final String pluginName;

  public ArtifactSelectorProvider(String pluginType, String pluginName) {
    this.pluginType = pluginType;
    this.pluginName = pluginName;
  }

  /**
   * @return the plugin selector for this plugin. If artifact settings have been given, the selector will try to
   *         match the specified artifact settings using an {@link ArtifactSelector}.
   *         If not, the default {@link PluginSelector} is returned.
   */
  public PluginSelector getPluginSelector(@Nullable ArtifactSelectorConfig config) {
    return config == null ? new PluginSelector() : getArtifactSelector(config);
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
  private ArtifactSelector getArtifactSelector(ArtifactSelectorConfig config) {
    String name = config.getName();
    if (name != null && !nameMatcher.matchesAllOf(name)) {
      throw new IllegalArgumentException(String.format("'%s' is an invalid artifact name. " +
                                                         "Must contain only alphanumeric, '-', '.', or '_' characters.",
                                                       name));
    }

    String version = config.getVersion();
    ArtifactVersionRange range;
    try {
      range = ArtifactVersionRange.parse(version);
    } catch (InvalidArtifactRangeException e) {
      throw new IllegalArgumentException(String.format("%s is an invalid artifact version." +
                                                         "Must be an exact version or a version range " +
                                                         "with a lower and upper bound.", version));
    }

    String scope = config.getScope();
    ArtifactScope artifactScope = scope == null ? null : ArtifactScope.valueOf(scope.toUpperCase());
    return new ArtifactSelector(pluginType, pluginName, artifactScope, name, range);
  }
}
