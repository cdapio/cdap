/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link SystemMetadataWriter} that is provided with the metadata from an existing object.
 */
public class DelegateSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private final SystemMetadataProvider metadataProvider;

  public DelegateSystemMetadataWriter(MetadataPublisher metadataPublisher,
                                      NamespacedEntityId entity,
                                      SystemMetadataProvider metadataProvider) {
    super(metadataPublisher, entity);
    this.metadataProvider = metadataProvider;
  }

  @Override
  public Map<String, String> getSystemPropertiesToAdd() {
    return metadataProvider.getSystemPropertiesToAdd();
  }

  @Override
  public Set<String> getSystemTagsToAdd() {
    return metadataProvider.getSystemTagsToAdd();
  }

  @Nullable
  @Override
  public String getSchemaToAdd() {
    return metadataProvider.getSchemaToAdd();
  }
}
