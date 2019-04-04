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
 *
 */

package io.cdap.cdap.etl.proto.v2.validation;


import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An error that occurred due to the plugin for a stage not being found.
 */
public class PluginNotFoundError extends StageValidationError {
  private final String pluginType;
  private final String pluginName;
  private final ArtifactSelectorConfig requestedArtifact;
  private final ArtifactSelectorConfig suggestedArtifact;

  public PluginNotFoundError(String stage, String pluginType, String pluginName,
                             ArtifactSelectorConfig requestedArtifact) {
    this(stage, pluginType, pluginName, requestedArtifact, null);
  }

  public PluginNotFoundError(String stage, String pluginType, String pluginName,
                             ArtifactSelectorConfig requestedArtifact,
                             @Nullable ArtifactSelectorConfig suggestedArtifact) {
    super(Type.PLUGIN_NOT_FOUND,
          String.format("Plugin named '%s' of type '%s' not found.", pluginName, pluginType), stage);
    this.pluginType = pluginType;
    this.pluginName = pluginName;
    this.requestedArtifact = requestedArtifact;
    this.suggestedArtifact = suggestedArtifact;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    PluginNotFoundError that = (PluginNotFoundError) o;
    return Objects.equals(pluginType, that.pluginType) &&
      Objects.equals(pluginName, that.pluginName) &&
      Objects.equals(requestedArtifact, that.requestedArtifact) &&
      Objects.equals(suggestedArtifact, that.suggestedArtifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), pluginType, pluginName, requestedArtifact, suggestedArtifact);
  }
}
