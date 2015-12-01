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

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import java.util.Map;
import java.util.Set;

/**
 * Defines operations on {@link MetadataDataset} for both system and user metadata.
 *
 * Operations supported for a specified {@link Id.NamespacedId}:
 * <ul>
 *   <li>Adding new metadata: Supported for a single scope.</li>
 *   <li>Retrieving metadata: Supported for a specified scope as well as across both scopes.</li>
 *   <li>Removing metadata: Supported for a specified scope as well as across both scopes.</li>
 * </ul>
 */
public interface MetadataStore {

  /**
   * Adds/updates properties for the specified {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param entityId the {@link Id.NamespacedId} to add the properties to
   * @param properties the properties to add/update
   */
  void setProperties(MetadataScope scope, Id.NamespacedId entityId, Map<String, String> properties);

  /**
   * Adds tags for the specified {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add the tags in
   * @param entityId the {@link Id.NamespacedId} to add the tags to
   * @param tagsToAdd the tags to add
   */
  void addTags(MetadataScope scope, Id.NamespacedId entityId, String... tagsToAdd);

  /**
   * @return a set of {@link MetadataRecord} representing all the metadata (including properties and tags) for the
   * specified {@link Id.NamespacedId} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataRecord> getMetadata(Id.NamespacedId entityId);

  /**
   * @return a {@link MetadataRecord} representing all the metadata (including properties and tags) for the specified
   * {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   */
  MetadataRecord getMetadata(MetadataScope scope, Id.NamespacedId entityId);

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link Id.NamespacedId}s in the specified {@link MetadataScope}.
   */
  Set<MetadataRecord> getMetadata(MetadataScope scope, Set<Id.NamespacedId> entityIds);

  /**
   * @return the properties for the specified {@link Id.NamespacedId} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Map<String, String> getProperties(Id.NamespacedId entityId);

  /**
   * @return the properties for the specified {@link Id.NamespacedId} in the specified {@link MetadataScope}
   */
  Map<String, String> getProperties(MetadataScope scope, Id.NamespacedId entityId);

  /**
   * @return the tags for the specified {@link Id.NamespacedId} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Set<String> getTags(Id.NamespacedId entityId);

  /**
   * @return the tags for the specified {@link Id.NamespacedId} in the specified {@link MetadataScope}
   */
  Set<String> getTags(MetadataScope scope, Id.NamespacedId entityId);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link Id.NamespacedId} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @param entityId the {@link Id.NamespacedId} to remove all metadata for
   */
  void removeMetadata(Id.NamespacedId entityId);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link Id.NamespacedId} in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param entityId the {@link Id.NamespacedId} to remove all metadata for
   */
  void removeMetadata(MetadataScope scope, Id.NamespacedId entityId);

  /**
   * Removes all properties for the specified {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param entityId the {@link Id.NamespacedId} to remove all properties for
   */
  void removeProperties(MetadataScope scope, Id.NamespacedId entityId);

  /**
   * Removes the specified properties of the {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param entityId the {@link Id.NamespacedId} to remove the specified keys for
   * @param keys the keys to remove
   */
  void removeProperties(MetadataScope scope, Id.NamespacedId entityId, String... keys);

  /**
   * Removes tags of the {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param entityId the {@link Id.NamespacedId} to remove all tags for
   */
  void removeTags(MetadataScope scope, Id.NamespacedId entityId);

  /**
   * Removes the specified tags from the {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param entityId the {@link Id.NamespacedId} to remove the specified tags for
   * @param tagsToRemove the tags to remove
   */
  void removeTags(MetadataScope scope, Id.NamespacedId entityId, String ... tagsToRemove);

  /**
   * Search the Metadata Dataset in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery);

  /**
   * Search the Metadata Dataset in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to restrict the search to
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value]
   */
  Set<MetadataSearchResultRecord> searchMetadata(MetadataScope scope, String namespaceId, String searchQuery);

  /**
   * Search the Metadata Dataset for the specified target type in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}.
   *
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value]
   * @param type the {@link MetadataSearchTargetType} to restrict the search to
   */
  Set<MetadataSearchResultRecord> searchMetadataOnType(String namespaceId, String searchQuery,
                                                       MetadataSearchTargetType type);

  /**
   * Search the Metadata Dataset for the specified target type in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to restrict the search to
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value]
   * @param type the {@link MetadataSearchTargetType} to restrict the search to
   */
  Set<MetadataSearchResultRecord> searchMetadataOnType(MetadataScope scope, String namespaceId, String searchQuery,
                                                       MetadataSearchTargetType type);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in both {@link MetadataScope#USER}
   * and {@link MetadataScope#SYSTEM}.
   *
   * @param entityIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecord> getSnapshotBeforeTime(Set<Id.NamespacedId> entityIds, long timeMillis);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to get the snapshot in
   * @param entityIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope,  Set<Id.NamespacedId> entityIds, long timeMillis);
}
