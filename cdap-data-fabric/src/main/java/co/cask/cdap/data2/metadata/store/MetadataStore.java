/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Defines operations on {@link MetadataDataset} for both system and user metadata.
 *
 * Operations supported for a specified {@link MetadataEntity}:
 * <ul>
 *   <li>Adding new metadata: Supported for a single scope.</li>
 *   <li>Retrieving metadata: Supported for a specified scope as well as across both scopes.</li>
 *   <li>Removing metadata: Supported for a specified scope as well as across both scopes.</li>
 * </ul>
 */
public interface MetadataStore {

  /**
   * Adds/updates properties for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param metadataEntity the {@link MetadataEntity} to add the properties to
   * @param properties the properties to add/update
   */
  void setProperties(MetadataScope scope, MetadataEntity metadataEntity, Map<String, String> properties);

  /**
   * Sets the specified property for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to set/update the property in
   * @param metadataEntity the {@link MetadataEntity} to set the property for
   * @param key the property key
   * @param value the property value
   */
  void setProperty(MetadataScope scope, MetadataEntity metadataEntity, String key, String value);

  /**
   * Adds tags for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add the tags in
   * @param metadataEntity the {@link MetadataEntity} to add the tags to
   * @param tagsToAdd the tags to add
   */
  void addTags(MetadataScope scope, MetadataEntity metadataEntity, String... tagsToAdd);

  /**
   * @return a set of {@link MetadataRecordV2} representing all the metadata (including properties and tags) for the
   * specified {@link MetadataEntity} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity);

  /**
   * @return a {@link MetadataRecordV2} representing all the metadata (including properties and tags) for the specified
   * {@link MetadataEntity} in the specified {@link MetadataScope}.
   */
  MetadataRecordV2 getMetadata(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * @return a set of {@link MetadataRecordV2}s representing all the metadata (including properties and tags)
   * for the specified set of {@link MetadataEntity}s in the specified {@link MetadataScope}.
   */
  Set<MetadataRecordV2> getMetadata(MetadataScope scope, Set<MetadataEntity> metadataEntitys);

  /**
   * @return the properties for the specified {@link MetadataEntity} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Map<String, String> getProperties(MetadataEntity metadataEntity);

  /**
   * @return the properties for the specified {@link MetadataEntity} in the specified {@link MetadataScope}
   */
  Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * @return the tags for the specified {@link MetadataEntity} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Set<String> getTags(MetadataEntity metadataEntity);

  /**
   * @return the tags for the specified {@link MetadataEntity} in the specified {@link MetadataScope}
   */
  Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link MetadataEntity} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove all metadata for
   */
  void removeMetadata(MetadataEntity metadataEntity);

  /**
   * Removes all metadata (including properties and tags) for the specified {@link MetadataEntity} in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param metadataEntity the {@link MetadataEntity} to remove all metadata for
   */
  void removeMetadata(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * Removes all properties for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param metadataEntity the {@link MetadataEntity} to remove all properties for
   */
  void removeProperties(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * Removes the specified properties of the {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param metadataEntity the {@link MetadataEntity} to remove the specified keys for
   * @param keys the keys to remove
   */
  void removeProperties(MetadataScope scope, MetadataEntity metadataEntity, String... keys);

  /**
   * Removes tags of the {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param metadataEntity the {@link MetadataEntity} to remove all tags for
   */
  void removeTags(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * Removes the specified tags from the {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param metadataEntity the {@link MetadataEntity} to remove the specified tags for
   * @param tagsToRemove the tags to remove
   */
  void removeTags(MetadataScope scope, MetadataEntity metadataEntity, String ... tagsToRemove);

  /**
   * Search the Metadata Dataset for the specified target types in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}.
   *
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value]
   * @param types the {@link EntityTypeSimpleName} to restrict the search to, if empty all types are searched
   * @param sortInfo represents sorting information. Use {@link SortInfo#DEFAULT} to return search results without
   *                 sorting (which implies that the sort order is by relevance to the search query)
   * @param offset the index to start with in the search results. To return results from the beginning, pass {@code 0}
   * @param limit the number of results to return, starting from #offset. To return all, pass {@link Integer#MAX_VALUE}
   * @param numCursors the number of cursors to return in the response. A cursor identifies the first index of the
   *                   next page for pagination purposes. Defaults to {@code 0}
   * @param cursor the cursor that acts as the starting index for the requested page. This is only applicable when
   *               #sortInfo is not {@link SortInfo#DEFAULT}. If offset is also specified, it is applied starting at
   *               the cursor. If {@code null}, the first row is used as the cursor
   * @param showHidden boolean which specifies whether to display hidden entities (entity whose name start with "_")
   *                    or not.
   * @param entityScope a set which specifies which scope of entities to display.
   * @return the {@link MetadataSearchResponse} containing search results for the specified search query and filters
   */
  MetadataSearchResponse search(String namespaceId, String searchQuery,
                                Set<EntityTypeSimpleName> types,
                                SortInfo sortInfo, int offset, int limit,
                                int numCursors, String cursor, boolean showHidden,
                                Set<EntityScope> entityScope);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in both {@link MetadataScope#USER}
   * and {@link MetadataScope#SYSTEM}.
   *
   * @param metadataEntitys entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecordV2> getSnapshotBeforeTime(Set<MetadataEntity> metadataEntitys, long timeMillis);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to get the snapshot in
   * @param metadataEntitys entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecordV2> getSnapshotBeforeTime(MetadataScope scope, Set<MetadataEntity> metadataEntitys,
                                              long timeMillis);

  /**
   * Rebuild stale metadata indexes.
   */
  void rebuildIndexes(MetadataScope scope, RetryStrategy retryStrategy);

  /**
   * Delete all existing metadata indexes.
   */
  void deleteAllIndexes(MetadataScope scope) throws DatasetManagementException, IOException;

  /**
   * Creates the MetadataDataset if its not already created. Otherwise, upgrades it if required.
   * @param scope of the MetadataDataset
   * @throws DatasetManagementException when dataset service is not available
   * @throws IOException when creation of dataset instance using its admin fails
   */
  void createOrUpgrade(MetadataScope scope) throws DatasetManagementException, IOException;

  /**
   * Creates a special tag in the MetadataDataset to mark the upgrade status of the MetadataDataset
   * with the current CDAP Version
   * @throws DatasetManagementException when dataset service is not available
   * @throws IOException when creation of dataset instance using its admin fails
   */
  void markUpgradeComplete(MetadataScope scope) throws DatasetManagementException, IOException;

  /**
   * Uses MetadataDataset to check if the version of the MetadataDataset is same as the
   * current CDAP version
   * @return true if upgrade is required, false otherwise
   * @throws DatasetManagementException when dataset service is not available
   */
  boolean isUpgradeRequired(MetadataScope scope) throws DatasetManagementException;
}
