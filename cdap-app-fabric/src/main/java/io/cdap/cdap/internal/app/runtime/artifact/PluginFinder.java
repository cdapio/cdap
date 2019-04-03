/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.Map;

/**
 * Interface for finding plugin.
 */
public interface PluginFinder {

  /**
   * Finds a plugin in the given namespace as well as in the SYSTEM namespace.
   *
   * @param pluginNamespaceId the namespace that the plugin is deployed in.
   * @param parentArtifactId the artifact that the plugin extends
   * @param pluginType type of the plugin
   * @param pluginName name of the plugin
   * @param selector a {@link PluginSelector} to select a plugin
   * @return a pair of {@link ArtifactDescriptor} and {@link PluginClass}
   * @throws PluginNotExistsException if no plugin can be found
   */
  Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId pluginNamespaceId,
                                                        ArtifactId parentArtifactId,
                                                        String pluginType, String pluginName,
                                                        PluginSelector selector) throws PluginNotExistsException;

}
