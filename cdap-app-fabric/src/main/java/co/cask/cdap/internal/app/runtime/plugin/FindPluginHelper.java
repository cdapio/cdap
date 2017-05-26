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
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

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
   * Find and return the requested plugin using the artifact repository, if found.
   * @param artifactRepository artifact repository to find plugin artifact.
   * @param pluginInstantiator plugin instantiator to add the identified plugin artifact.
   * @param namespace namespace of plugin
   * @param parentArtifactId parent artifact
   * @param pluginType plugin type
   * @param pluginName name of the plugin
   * @param properties plugin properties
   * @param selector seelector used to select a plugin
   * @return {@link Plugin}
   * @throws PluginNotExistsException
   * @throws ArtifactNotFoundException
   */
  public static Plugin findPlugin(ArtifactRepository artifactRepository,
                              PluginInstantiator pluginInstantiator,
                              NamespaceId namespace,
                              Id.Artifact parentArtifactId, String pluginType, String pluginName,
                              PluginProperties properties, PluginSelector selector)
    throws PluginNotExistsException, ArtifactNotFoundException {
    return findPlugin(artifactRepository, pluginInstantiator, namespace,
                      new ArtifactRange(parentArtifactId.getNamespace().getId(), parentArtifactId.getName(),
                                        parentArtifactId.getVersion(), true,
                                        parentArtifactId.getVersion(), true),
                      pluginType, pluginName, properties, selector);
  }


  /**
   * Find and return the requested plugin using the artifact repository, if found.
   * @param artifactRepository artifact repository to find plugin artifact.
   * @param pluginInstantiator plugin instantiator to add the identified plugin artifact.
   * @param namespace namespace of plugin
   * @param parentArtifactRange parent artifact range
   * @param pluginType plugin type
   * @param pluginName name of the plugin
   * @param properties plugin properties
   * @param selector seelector used to select a plugin
   * @return {@link Plugin}
   * @throws PluginNotExistsException
   * @throws ArtifactNotFoundException
   */
  public static Plugin findPlugin(ArtifactRepository artifactRepository,
                                  PluginInstantiator pluginInstantiator,
                                  NamespaceId namespace,
                                  ArtifactRange parentArtifactRange, String pluginType, String pluginName,
                                  PluginProperties properties, PluginSelector selector)
    throws PluginNotExistsException, ArtifactNotFoundException {
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");
    Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry;
    try {
      pluginEntry = artifactRepository.findPlugin(namespace, parentArtifactRange, pluginType, pluginName, selector);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return getPlugin(pluginEntry, properties, pluginType, pluginName, pluginInstantiator);
  }

  private static Plugin getPlugin(Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry, PluginProperties properties,
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
    return new Plugin(artifact, pluginEntry.getValue(),
                      properties.setMacros(collectMacroEvaluator.getMacros()));
  }

}

