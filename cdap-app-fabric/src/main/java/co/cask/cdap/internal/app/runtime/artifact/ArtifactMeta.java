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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.templates.plugins.PluginClass;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Metadata about an artifact, such as what plugins are contained in the artifact.
 */
public class ArtifactMeta {
  private final List<PluginClass> plugins;

  public ArtifactMeta(List<PluginClass> plugins) {
    this.plugins = ImmutableList.copyOf(plugins);
  }

  public List<PluginClass> getPlugins() {
    return plugins;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactMeta that = (ArtifactMeta) o;

    return Objects.equals(plugins, that.plugins);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(plugins);
  }
}
