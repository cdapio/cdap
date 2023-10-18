/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app;

import com.google.common.collect.Maps;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.FindPluginHelper;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.common.PluginNotExistsException;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.twill.filesystem.Location;

/**
 * Remote plugin configurer when the program is run in ISOLATED mode
 */
public class RemotePluginConfigurer extends DefaultPluginConfigurer {

  private final Map<String, Plugin> localPlugins;
  private final File localPluginDir;

  public RemotePluginConfigurer(ArtifactId artifactId, NamespaceId pluginNamespaceId,
      PluginInstantiator pluginInstantiator, PluginFinder pluginFinder,
      Map<String, Plugin> localPlugins, String localPluginDir) {
    super(artifactId, pluginNamespaceId, pluginInstantiator, pluginFinder);
    this.localPlugins = localPlugins;
    this.localPluginDir = new File(localPluginDir);
  }

  @Override
  protected Plugin addPlugin(String pluginType, String pluginName, String pluginId,
      PluginProperties properties, PluginSelector selector) throws PluginNotExistsException {
    validateExistingPlugin(pluginId);
    // this means this plugin is not registered when the app is initially deployed, use the normal way to find the
    // plugin
    if (!localPlugins.containsKey(pluginId)) {
      return super.addPlugin(pluginType, pluginName, pluginId, properties, selector);
    }

    Plugin existingPlugin = localPlugins.get(pluginId);
    File existingPluginLocation = new File(localPluginDir,
        Artifacts.getFileName(existingPlugin.getArtifactId()));

    // need to regenerate this plugin to ensure the plugin has updated properties with macro resolved, also
    // register it to plugin instantiator
    io.cdap.cdap.api.artifact.ArtifactId artifactId = existingPlugin.getArtifactId();
    String namespace =
        artifactId.getScope().equals(ArtifactScope.SYSTEM) ? NamespaceId.SYSTEM.getNamespace() :
            pluginNamespaceId.getNamespace();
    Location pluginLocation = Locations.toLocation(existingPluginLocation);

    SortedMap<io.cdap.cdap.api.artifact.ArtifactId, PluginClass> selectedPlugin = new TreeMap<>();
    selectedPlugin.put(artifactId, existingPlugin.getPluginClass());
    selector.select(selectedPlugin);

    Plugin plugin = FindPluginHelper.getPlugin(
        existingPlugin.getParents(),
        Maps.immutableEntry(new ArtifactDescriptor(namespace, artifactId, pluginLocation),
            existingPlugin.getPluginClass()), properties, pluginInstantiator);
    plugins.put(pluginId, new PluginWithLocation(plugin, pluginLocation));
    return plugin;
  }
}
