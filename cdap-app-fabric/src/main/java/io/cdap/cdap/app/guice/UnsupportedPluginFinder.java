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

package io.cdap.cdap.app.guice;

import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.common.PluginNotExistsException;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.Map;

/**
 * A {@link PluginFinder} implementation that throws {@link UnsupportedOperationException} on every
 * method call. This is used in runtime environment that dynamic plugin loading is not supported.
 */
public class UnsupportedPluginFinder implements PluginFinder {

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(NamespaceId pluginNamespaceId,
      ArtifactId parentArtifactId, String pluginType,
      String pluginName, PluginSelector selector)
      throws PluginNotExistsException {

    throw new UnsupportedOperationException("Dynamic plugin configuration is not supported");
  }
}
