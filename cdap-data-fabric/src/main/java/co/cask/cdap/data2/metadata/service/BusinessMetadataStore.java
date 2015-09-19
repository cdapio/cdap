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

package co.cask.cdap.data2.metadata.service;

import co.cask.cdap.data2.metadata.dataset.BusinessMetadataDataset;
import co.cask.cdap.data2.metadata.dataset.BusinessMetadataRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import java.util.Map;
import java.util.Set;

/**
 * Define operations on {@link BusinessMetadataDataset} transactionally.
 */
public interface BusinessMetadataStore {

  /**
   * Adds/updates metadata for the specified {@link Id.NamespacedId}.
   */
  void setProperties(final Id.NamespacedId entityId, final Map<String, String> properties);

  /**
   * Adds tags for the specified {@link Id.NamespacedId}.
   */
  void addTags(final Id.NamespacedId entityId, final String ... tagsToAdd);

  /**
   * @return a {@link MetadataRecord} representing all the metadata (including properties and tags) for the specified
   * {@link Id.NamespacedId}.
   */
  MetadataRecord getMetadata(final Id.NamespacedId entityId);

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link Id.NamespacedId}s.
   */
  Set<MetadataRecord> getMetadata(final Set<Id.NamespacedId> entityIds);

  /**
   * @return the metadata for the specified {@link Id.NamespacedId}
   */
  Map<String, String> getProperties(final Id.NamespacedId entityId);

  /**
   * @return the tags for the specified {@link Id.NamespacedId}
   */
  Set<String> getTags(final Id.NamespacedId entityId);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link Id.NamespacedId}.
   */
  void removeMetadata(final Id.NamespacedId entityId);

  /**
   * Removes all properties for the specified {@link Id.NamespacedId}.
   */
  void removeProperties(final Id.NamespacedId entityId);

  /**
   * Removes the specified properties of the {@link Id.NamespacedId}.
   */
  void removeProperties(final Id.NamespacedId entityId, final String... keys);

  /**
   * Removes tags of the {@link Id.NamespacedId}.
   */
  void removeTags(final Id.NamespacedId entityId);

  /**
   * Removes the specified tags from the {@link Id.NamespacedId}
   */
  void removeTags(final Id.NamespacedId entityId, final String ... tagsToRemove);

  /**
   * Search to the underlying Business Metadata Dataset.
   */
  Iterable<BusinessMetadataRecord> searchMetadata(String namespaceId, String searchQuery);

  /**
   * Search to the underlying Business Metadata Dataset for a target type.
   */
  Iterable<BusinessMetadataRecord> searchMetadataOnType(String namespaceId, String searchQuery,
                                                        MetadataSearchTargetType type);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time.
   * @param entityIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecord> getSnapshotBeforeTime(Set<Id.NamespacedId> entityIds, long timeMillis);
}
