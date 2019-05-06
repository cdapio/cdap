/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.system;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.spi.metadata.MetadataConstants;

import java.util.Map;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Artifact artifact}.
 */
public class ArtifactSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private final ArtifactInfo artifactInfo;

  public ArtifactSystemMetadataWriter(MetadataServiceClient metadataServiceClient, ArtifactId artifactId,
                                      ArtifactInfo artifactInfo) {
    super(metadataServiceClient, artifactId);
    this.artifactInfo = artifactInfo;
  }

  @Override
  public Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(MetadataConstants.ENTITY_NAME_KEY, artifactInfo.getName());
    ArtifactClasses classes = artifactInfo.getClasses();
    for (PluginClass pluginClass : classes.getPlugins()) {
      SystemMetadataProvider.addPlugin(pluginClass, artifactInfo.getVersion(), properties);
    }
    properties.put(MetadataConstants.CREATION_TIME_KEY, String.valueOf(System.currentTimeMillis()));
    return properties.build();
  }
}
