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

package co.cask.cdap.metadata;

import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Interface that the {@link MetadataHttpHandler} uses to interact with Metadata.
 * All the create, update and remove operations through this interface are restricted to {@link MetadataScope#USER},
 * so that clients of the RESTful API cannot create, update or remove {@link MetadataScope#SYSTEM} metadata.
 * The operations to retrieve metadata properties and tags allow passing in a scope, so clients of the RESTful API
 * can retrieve both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} metadata.
 */
public interface MetadataAdmin {

  /**
   * Adds the specified {@link Map} to the metadata of the specified {@link Id.NamespacedId entityId}.
   * Existing keys are updated with new values, newer keys are appended to the metadata. This API only supports adding
   * properties in {@link MetadataScope#USER}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addProperties(Id.NamespacedId entityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException;

  /**
   * Adds the specified tags to specified {@link Id.NamespacedId}. This API only supports adding tags in
   * {@link MetadataScope#USER}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException, InvalidMetadataException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link Id.NamespacedId} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link Id.NamespacedId} in the specified {@link MetadataScope}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: Should this return a single metadata record instead or is a set of one record ok?
  Set<MetadataRecord> getMetadata(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link Id.NamespacedId} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: This should perhaps return a Map<MetadataScope, Map<String, String>>
  Map<String, String> getProperties(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link Id.NamespacedId} in the specified
   * {@link MetadataScope}
   * @throws NotFoundException if the specified entity was not found
   */
  Map<String, String> getProperties(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return all the tags for the specified {@link Id.NamespacedId} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: This should perhaps return a Map<MetadataScope, Set<String>>
  Set<String> getTags(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return all the tags for the specified {@link Id.NamespacedId} in the specified {@link MetadataScope}
   * @throws NotFoundException if the specified entity was not found
   */
  Set<String> getTags(MetadataScope scope, Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes all the metadata (including properties and tags) for the specified {@link Id.NamespacedId}. This
   * API only supports removing metadata in {@link MetadataScope#USER}.
   *
   * @param entityId the {@link Id.NamespacedId} to remove metadata for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeMetadata(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes all properties from the metadata of the specified {@link Id.NamespacedId}. This API only supports
   * removing properties in {@link MetadataScope#USER}.
   *
   * @param entityId the {@link Id.NamespacedId} to remove properties for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes the specified keys from the metadata properties of the specified {@link Id.NamespacedId}. This API only
   * supports removing properties in {@link MetadataScope#USER}.
   *
   * @param entityId the {@link Id.NamespacedId} to remove the specified properties for
   * @param keys the metadata property keys to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(Id.NamespacedId entityId, String... keys) throws NotFoundException;

  /**
   * Removes all tags from the specified {@link Id.NamespacedId}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param entityId the {@link Id.NamespacedId} to remove tags for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.NamespacedId}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param entityId the {@link Id.NamespacedId} to remove the specified tags for
   * @param tags the tags to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(Id.NamespacedId entityId, String ... tags) throws NotFoundException;

  /**
   * Executes a search for CDAP entities in the specified namespace with the specified search query and an optional
   * {@link MetadataSearchTargetType entity type} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @param namespaceId The namespace to filter the search by
   * @param searchQuery The search query
   * @param type The particular type of CDAP entity to be searched. If null all possible types will be searched
   * @return a {@link Set} containing a {@link MetadataSearchResultRecord} for each matching entity
   */
  Set<MetadataSearchResultRecord> searchMetadata(String namespaceId, String searchQuery,
                                                 @Nullable MetadataSearchTargetType type);

  /**
   * Executes a search for CDAP entities in the specified namespace with the specified search query and an optional
   * {@link MetadataSearchTargetType entity type} in the specified {@link MetadataScope}.
   *
   * @param scope the {@link MetadataScope} to restrict the search to
   * @param namespaceId The namespace id to filter the search by
   * @param searchQuery The search query
   * @param type The particular type of CDAP entity to be searched. If null all possible types will be searched
   * @return a {@link Set} containing a {@link MetadataSearchResultRecord} for each matching entity
   */
  Set<MetadataSearchResultRecord> searchMetadata(MetadataScope scope, String namespaceId, String searchQuery,
                                                 @Nullable MetadataSearchTargetType type);
}
