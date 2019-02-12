/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Defines metadata operations for both system and user scope.
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
   * Create all tables or indexes required for operations.
   */
  void createIndex() throws IOException;

  /**
   * Drop all tables or indexes required for operations.
   */
  void dropIndex() throws IOException;

  /**
   * Replaces the metadata for the given entity: All existing properties and tags are
   * replaced with the given Metadata, with these exceptions: some existing properties
   * are kept even if the new properties do not contain them, and some properties are
   * left unchanged even if the new properties contain a new value.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param metadata the record of the new metadata, including the entity, properties and tags
   * @param propertiesToKeep the names of properties that should be kept even if the
   *                         new metadata does not contain them
   * @param propertiesToPreserve the names of properties to leave unchanged even if
   *                             the new metadata contains no or new values for them
   */
  void replaceMetadata(MetadataScope scope, MetadataDataset.Record metadata,
                       Set<String> propertiesToKeep, Set<String> propertiesToPreserve);

  /**
   * Adds/updates properties for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param metadataEntity the {@link MetadataEntity} to add the properties to
   * @param properties the properties to add/update
   */
  void addProperties(MetadataScope scope, MetadataEntity metadataEntity, Map<String, String> properties);

  /**
   * Adds/updates properties for each specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add/update the properties in
   * @param toUpdate the properties to add/update, for each entity in the map
   */
  void addProperties(MetadataScope scope, Map<MetadataEntity, Map<String, String>> toUpdate);

  /**
   * Adds the specified property for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to set/update the property in
   * @param metadataEntity the {@link MetadataEntity} to set the property for
   * @param key the property key
   * @param value the property value
   */
  void addProperty(MetadataScope scope, MetadataEntity metadataEntity, String key, String value);

  /**
   * Adds tags for the specified {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to add the tags in
   * @param metadataEntity the {@link MetadataEntity} to add the tags to
   * @param tagsToAdd the tags to add
   */
  void addTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToAdd);

  /**
   * @return a set of {@link MetadataRecord} representing all the metadata (including properties and tags) for the
   * specified {@link MetadataEntity} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity);

  /**
   * @return a {@link MetadataRecord} representing all the metadata (including properties and tags) for the specified
   * {@link MetadataEntity} in the specified {@link MetadataScope}.
   */
  MetadataRecord getMetadata(MetadataScope scope, MetadataEntity metadataEntity);

  /**
   * @return a set of {@link MetadataRecord}s representing all the metadata (including properties and tags)
   * for the specified set of {@link MetadataEntity}s in the specified {@link MetadataScope}.
   */
  Set<MetadataRecord> getMetadata(MetadataScope scope, Set<MetadataEntity> metadataEntitys);

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
  void removeProperties(MetadataScope scope, MetadataEntity metadataEntity, Set<String> keys);

  /**
   * Removes the specified properties for each {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope}
   * @param toRemove a map specifying the set of keys to remove for each metadata entity.
   */
  void removeProperties(MetadataScope scope, Map<MetadataEntity, Set<String>> toRemove);

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
  void removeTags(MetadataScope scope, MetadataEntity metadataEntity, Set<String> tagsToRemove);

  /**
   * Search the Metadata Dataset for the specified target types in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}.
   *
   * @param request the {@link SearchRequest}
   * @return the {@link MetadataSearchResponse} containing search results for the specified search query and filters
   */
  MetadataSearchResponse search(SearchRequest request);

  /**
   * Returns the snapshot of the metadata for entities on or before the given time in the specified
   * {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to get the snapshot in
   * @param metadataEntities entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  Set<MetadataRecord> getSnapshotBeforeTime(MetadataScope scope, Set<MetadataEntity> metadataEntities,
                                            long timeMillis);

}
