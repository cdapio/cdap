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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of {@link PluginFinder} that uses {@link ArtifactRepository} directly.
 */
public class LocalPluginFinder implements PluginFinder {

  private final ArtifactRepository artifactRepository;

  public LocalPluginFinder(ArtifactRepository artifactRepository) {
    this.artifactRepository = artifactRepository;
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId pluginNamespaceId, ArtifactRange parentRange,
                                                               String pluginType, String pluginName,
                                                               PluginSelector selector)
    throws PluginNotExistsException {
    try {
      return artifactRepository.findPlugin(pluginNamespaceId, parentRange, pluginType, pluginName, selector);
    } catch (IOException | ArtifactNotFoundException e) {
      // If there is error accessing artifact store or if the parent artifact is missing, just propagate
      throw Throwables.propagate(e);
    }
  }
}
