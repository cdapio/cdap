/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Defines operations on {@link MetadataDataset} for both system and user metadata.
 *
 * Operations supported for a specified {@link NamespacedEntityId}:
 * <ul>
 *   <li>Adding new metadata: Supported for a single scope.</li>
 *   <li>Retrieving metadata: Supported for a specified scope as well as across both scopes.</li>
 *   <li>Removing metadata: Supported for a specified scope as well as across both scopes.</li>
 * </ul>
 */
public interface MetadataStore {

  /**
   * Adds/updates properties for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param namespacedEntityId the {@link NamespacedEntityId} to add the properties to
   * @param properties the properties to add/update
   */
  void setProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId, Map<String, String> properties);

  /**
   * Adds/updates properties for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param namespacedEntityId the {@link NamespacedEntityId} to add the properties to
   * @param properties the properties to add/update
   * @param indexer {@link Indexer} to use for creating indexes
   */
  void setProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId, Map<String, String> properties,
                     @Nullable Indexer indexer);


  /**
   * Adds tags for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add the tags in
   * @param namespacedEntityId the {@link NamespacedEntityId} to add the tags to
   * @param tagsToAdd the tags to add
   */
  void addTags(MetadataScope scope, NamespacedEntityId namespacedEntityId, String... tagsToAdd);

  /**
   * @return a set of {@link MetadataRecord} representing all the metadata (including properties and tags) for the
   * specified {@link NamespacedEntityId} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataRecord> getMetadata(NamespacedEntityId namespacedEntityId);

  /**
   * @return a {@link MetadataRecord} representing all the metadata (including properties and tags) for the specified
   * {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   */
  MetadataRecord getMetadata(MetadataScope scope, NamespacedEntityId namespacedEntityId);

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link NamespacedEntityId}s in the specified {@link MetadataScope}.
   */
  Set<MetadataRecord> getMetadata(MetadataScope scope, Set<NamespacedEntityId> namespacedEntityIds);

  /**
   * @return the properties for the specified {@link NamespacedEntityId} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Map<String, String> getProperties(NamespacedEntityId namespacedEntityId);

  /**
   * @return the properties for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}
   */
  Map<String, String> getProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId);

  /**
   * @return the tags for the specified {@link NamespacedEntityId} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Set<String> getTags(NamespacedEntityId namespacedEntityId);

  /**
   * @return the tags for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}
   */
  Set<String> getTags(MetadataScope scope, NamespacedEntityId namespacedEntityId);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link NamespacedEntityId} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove all metadata for
   */
  void removeMetadata(NamespacedEntityId namespacedEntityId);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link NamespacedEntityId} in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove all metadata for
   */
  void removeMetadata(MetadataScope scope, NamespacedEntityId namespacedEntityId);

  /**
   * Removes all properties for the specified {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove all properties for
   */
  void removeProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId);

  /**
   * Removes the specified properties of the {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove the specified keys for
   * @param keys the keys to remove
   */
  void removeProperties(MetadataScope scope, NamespacedEntityId namespacedEntityId, String... keys);

  /**
   * Removes tags of the {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove all tags for
   */
  void removeTags(MetadataScope scope, NamespacedEntityId namespacedEntityId);

  /**
   * Removes the specified tags from the {@link NamespacedEntityId} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param namespacedEntityId the {@link NamespacedEntityId} to remove the specified tags for
   * @param tagsToRemove the tags to remove
   */
  void removeTags(MetadataScope scope, NamespacedEntityId namespacedEntityId, String ... tagsToRemove);

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
   * Search the Metadata Dataset for the specified target types in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}.
   *
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value]
   * @param types the {@link MetadataSearchTargetType} to restrict the search to, if empty all types are searched
   */
  Set<MetadataSearchResultRecord> searchMetadataOnType(String namespaceId, String searchQuery,
                                                       Set<MetadataSearchTargetType> types);

  /**
   * Search the Metadata Dataset for the specified target types in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to restrict the search to
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value]
   * @param types the {@link MetadataSearchTargetType} to restrict the search to, if empty all types are searched
   */
  Set<MetadataSearchResultRecord> searchMetadataOnType(MetadataScope scope, String namespaceId, String searchQuery,
                                                       Set<MetadataSearchTargetType> types);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in both {@link MetadataScope#USER}
   * and {@link MetadataScope#SYSTEM}.
   *
   * @param namespacedEntityIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecord> getSnapshotBeforeTime(Set<NamespacedEntityId> namespacedEntityIds, long timeMillis);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to get the snapshot in
   * @param namespacedEntityIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, Set<NamespacedEntityId> namespacedEntityIds,
                                            long timeMillis);

  /**
   * Rebuild stale metadata indexes.
   */
  void rebuildIndexes();

  /**
   * Delete all existing metadata indexes.
   */
  void deleteAllIndexes();
}
