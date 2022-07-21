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

import com.google.common.base.Strings;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataMutation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A class to write {@link MetadataScope#SYSTEM} metadata for an {@link NamespacedEntityId entity}.
 */
public abstract class AbstractSystemMetadataWriter implements SystemMetadataWriter, SystemMetadataProvider {

  private final MetadataServiceClient metadataServiceClient;
  private final MetadataEntity metadataEntity;

  AbstractSystemMetadataWriter(MetadataServiceClient metadataServiceClient, NamespacedEntityId entityId) {
    this.metadataServiceClient = metadataServiceClient;
    this.metadataEntity = entityId.toMetadataEntity();
  }

  /**
   * Updates the {@link MetadataScope#SYSTEM} metadata for this {@link NamespacedEntityId entity}.
   */
  @Override
  public void write() {
    metadataServiceClient.create(getMetadataMutation());
  }

  public MetadataMutation.Create getMetadataMutation() {
    String schema = getSchemaToAdd();
    Set<String> tags = getSystemTagsToAdd();
    Map<String, String> properties = getSystemPropertiesToAdd();
    if (!Strings.isNullOrEmpty(schema)) {
      properties = new HashMap<>(properties);
      properties.put(MetadataConstants.SCHEMA_KEY, schema);
    }
    return new MetadataMutation.Create(metadataEntity, new Metadata(MetadataScope.SYSTEM, tags, properties),
                                       MetadataMutation.Create.CREATE_DIRECTIVES);
  }
}
