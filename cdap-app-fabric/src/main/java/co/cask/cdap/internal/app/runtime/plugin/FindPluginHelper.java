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

package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.ArrayList;
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
   * @param pluginType plugin type
   * @param pluginName plugin name
   * @param pluginInstantiator instantiator to add the plugin artifact to
   * @return plugin information
   */
  public static Plugin getPlugin(Iterable<ArtifactId> parents, Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry,
                                 PluginProperties properties,
                                 String pluginType, String pluginName, PluginInstantiator pluginInstantiator) {
    CollectMacroEvaluator collectMacroEvaluator = new CollectMacroEvaluator();

    // Just verify if all required properties are provided.
    // No type checking is done for now.
    for (PluginPropertyField field : pluginEntry.getValue().getProperties().values()) {
      Preconditions.checkArgument(!field.isRequired() || (properties.getProperties().containsKey(field.getName())),
                                  "Required property '%s' missing for plugin of type %s, name %s.",
                                  field.getName(), pluginType, pluginName);
      if (field.isMacroSupported()) {
        MacroParser parser = new MacroParser(collectMacroEvaluator, field.isMacroEscapingEnabled());
        parser.parse(properties.getProperties().get(field.getName()));
      }
    }

    ArtifactId artifact = pluginEntry.getKey().getArtifactId();
    try {
      pluginInstantiator.addArtifact(pluginEntry.getKey().getLocation(), artifact);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return new Plugin(parents, artifact, pluginEntry.getValue(),
                      properties.setMacros(collectMacroEvaluator.getMacros()));
  }

}

