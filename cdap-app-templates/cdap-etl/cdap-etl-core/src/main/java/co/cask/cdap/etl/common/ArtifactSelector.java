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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersionRange;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Selects which plugin to use based on optional artifact scope, name, and version fields.
 * Will select the greatest artifact that matches all non-null fields.
 */
public class ArtifactSelector extends PluginSelector {
  private final ArtifactScope scope;
  private final String name;
  private final String errMsg;
  private final ArtifactVersionRange range;

  public ArtifactSelector(String pluginType,
                          String pluginName,
                          @Nullable ArtifactScope scope,
                          @Nullable String name,
                          @Nullable ArtifactVersionRange range) {
    this.scope = scope;
    this.name = name;
    this.range = range;
    StringBuilder msg = new StringBuilder("Could not find an artifact that matches");
    if (scope != null) {
      msg.append(" scope ");
      msg.append(scope.name());
    }
    if (name != null) {
      msg.append(" name ");
      msg.append(name);
    }
    if (range != null) {
      msg.append(range.getVersionString());
    }
    msg.append(" for plugin of type ");
    msg.append(pluginType);
    msg.append(" and name ");
    msg.append(pluginName);
    errMsg = msg.toString();
  }

  @Override
  public Map.Entry<ArtifactId, PluginClass> select(SortedMap<ArtifactId, PluginClass> plugins) {
    NavigableMap<ArtifactId, PluginClass> pluginMap;
    if (plugins instanceof NavigableMap) {
      pluginMap = (NavigableMap<ArtifactId, PluginClass>) plugins;
    } else {
      pluginMap = new TreeMap<>();
      pluginMap.putAll(plugins);
    }

    for (Map.Entry<ArtifactId, PluginClass> entry : pluginMap.descendingMap().entrySet()) {
      ArtifactId artifactId = entry.getKey();
      if ((scope == null || artifactId.getScope().equals(scope)) &&
        (name == null || artifactId.getName().equals(name)) &&
        (range == null || range.versionIsInRange(artifactId.getVersion()))) {
        return entry;
      }
    }

    throw new IllegalArgumentException(errMsg);
  }
}
