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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;

import java.io.IOException;
import java.util.Map;

/**
 * Abstract class that can help in finding plugin's
 */
public final class FindPluginHelper {

  private FindPluginHelper() {
    // no-op
  }

  /**
   * Get the Plugin information from the specified information.
   *
   * @param parents plugin parents of the plugin. Each item in the iterable must be the parent of the item before it,
   *                with the first item as the direct parent of the plugin
   * @param pluginEntry artifact and class information for the plugin
   * @param properties plugin properties
   * @param pluginInstantiator instantiator to add the plugin artifact to
   * @return plugin information
   */
  public static Plugin getPlugin(Iterable<ArtifactId> parents, Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry,
                                 PluginProperties properties, PluginInstantiator pluginInstantiator) {
    CollectMacroEvaluator collectMacroEvaluator = new CollectMacroEvaluator();

    for (PluginPropertyField field : pluginEntry.getValue().getProperties().values()) {
      if (field.isMacroSupported() && properties.getProperties().containsKey(field.getName())) {
        MacroParser parser = new MacroParser(collectMacroEvaluator,
                                             MacroParserOptions.builder()
                                               .setEscaping(field.isMacroEscapingEnabled())
                                               .build());
        parser.parse(properties.getProperties().get(field.getName()));
      }
    }

    ArtifactId artifact = pluginEntry.getKey().getArtifactId();
    try {
      pluginInstantiator.addArtifact(pluginEntry.getKey().getLocation(), artifact);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return new Plugin(parents, artifact, pluginEntry.getValue(),
                      properties.setMacros(collectMacroEvaluator.getMacros()));
  }

}

