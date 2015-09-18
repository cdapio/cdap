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

package co.cask.cdap.metadata;

import co.cask.cdap.common.InvalidMetadataException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Interface to interact with Metadata.
 */
public interface MetadataAdmin {

  /**
   * Adds the specified {@link Map} to the business metadata of the specified {@link Id.Application},
   * {@link Id.Program}, {@link Id.DatasetInstance} or {@link Id.Stream}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addProperties(Id.NamespacedId entityId, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException;

  /**
   * Adds the specified tags to specified {@link Id.Application}, {@link Id.Program}, {@link Id.DatasetInstance} or
   * {@link Id.Stream}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException, InvalidMetadataException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link Id.NamespacedId}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  Set<MetadataRecord> getMetadata(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return a {@link Map} representing the business metadata of the specified {@link Id.Application},
   * {@link Id.Program}, {@link Id.DatasetInstance} or {@link Id.Stream}
   * @throws NotFoundException if the specified entity was not found
   */
  Map<String, String> getProperties(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return all the tags for the specified {@link Id.Application}, {@link Id.Program}, {@link Id.DatasetInstance} or
   * {@link Id.Stream}
   * @throws NotFoundException if the specified entity was not found
   */
  Iterable<String> getTags(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes all the business metadata (including properties and tags) for the specified {@link Id.NamespacedId}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void removeMetadata(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes all properties from the business metadata of the specified {@link Id.Application}, {@link Id.Program},
   * {@link Id.DatasetInstance} or {@link Id.Stream}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.Application}, {@link Id.Program},
   * {@link Id.DatasetInstance} or {@link Id.Stream}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(Id.NamespacedId entityId, String... keys) throws NotFoundException;

  /**
   * Removes all tags from the specified {@link Id.Application}, {@link Id.Program},
   * {@link Id.DatasetInstance} or {@link Id.Stream}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.Application}, {@link Id.Program},
   * {@link Id.DatasetInstance} or {@link Id.Stream}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(Id.NamespacedId entityId, String ... tags) throws NotFoundException;

  /**
   * Execute search for metadata for particular type of CDAP object.
   *
   * @param searchQuery The query need to be executed for the search.
   * @param type The particular type of CDAP object that the metadata need to be searched. If null all possible types
   *             will be searched.
   *
   * @return a {@link Set} records for metadata search.
   * @throws NotFoundException if there is not record found for particular query text.
   */
  Set<MetadataSearchResultRecord> searchMetadata(String searchQuery, @Nullable MetadataSearchTargetType type)
    throws NotFoundException;
}
