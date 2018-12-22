/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.data2.metadata.writer.MetadataOperation;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A class to write {@link MetadataScope#SYSTEM} metadata for an {@link NamespacedEntityId entity}.
 */
public abstract class AbstractSystemMetadataWriter implements SystemMetadataWriter, SystemMetadataProvider {

  private final MetadataPublisher metadataPublisher;
  private final MetadataEntity metadataEntity;

  AbstractSystemMetadataWriter(MetadataPublisher metadataPublisher, NamespacedEntityId entityId) {
    this.metadataPublisher = metadataPublisher;
    this.metadataEntity = entityId.toMetadataEntity();
  }

  /**
   * Updates the {@link MetadataScope#SYSTEM} metadata for this {@link NamespacedEntityId entity}.
   */
  @Override
  public void write() {
    String schema = getSchemaToAdd();
    Set<String> tags = getSystemTagsToAdd();
    Map<String, String> properties = getSystemPropertiesToAdd();
    if (!Strings.isNullOrEmpty(schema)) {
      properties = new HashMap<>(properties);
      properties.put(SystemMetadataProvider.SCHEMA_KEY, schema);
    }
    metadataPublisher.publish(NamespaceId.SYSTEM, new MetadataOperation.Create(metadataEntity, properties, tags));
  }
}
