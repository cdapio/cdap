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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Application application}.
 */
public class AppSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private ApplicationSpecification appSpec;

  public AppSystemMetadataWriter(MetadataStore metadataStore, Id.Application entityId,
                                 ApplicationSpecification appSpec) {
    super(metadataStore, entityId);
    this.appSpec = appSpec;
  }

  @Override
  Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    for (Map.Entry<String, Plugin> pluginEntry : appSpec.getPlugins().entrySet()) {
      String pluginId = pluginEntry.getKey();
      PluginClass pluginClass = pluginEntry.getValue().getPluginClass();
      properties.put("plugin" + CTRL_A + pluginId + CTRL_A + "type", pluginClass.getType());
      properties.put("plugin" + CTRL_A + pluginId + CTRL_A + "name", pluginClass.getName());
    }
    return properties.build();
  }

  @Override
  String[] getSystemTagsToAdd() {
    return new String[]{appSpec.getArtifactId().getName()};
  }
}
