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

package io.cdap.cdap.metadata;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.InvalidMetadataException;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;

import java.io.IOException;
import java.util.List;
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
   * Adds the specified {@link Map} to the metadata of the specified {@link MetadataEntity metadataEntity}.
   * Existing keys are updated with new values, newer keys are appended to the metadata. This API only supports adding
   * properties in {@link MetadataScope#USER}.
   *
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addProperties(MetadataEntity metadataEntity, Map<String, String> properties, MutationOptions options)
    throws InvalidMetadataException, IOException;

  /**
   * Adds the specified tags to specified {@link MetadataEntity}. This API only supports adding tags in
   * {@link MetadataScope#USER}.
   *
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addTags(MetadataEntity metadataEntity, Set<String> tags, MutationOptions options)
    throws InvalidMetadataException, IOException;

  /**
   * Returns all metadata (including properties and tags) for the specified {@link MetadataEntity}
   * in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Metadata getMetadata(MetadataEntity metadataEntity) throws IOException;

  /**
   * Returns the metadata (including properties and tags) for the specified {@link MetadataEntity}
   * in the specified {@link MetadataScope}.
   */
  Metadata getMetadata(MetadataEntity metadataEntity, MetadataScope scope) throws IOException;

  /**
   * Returns the metadata of specified kind (properties or tags) for the specified {@link MetadataEntity}
   * in the specified {@link MetadataScope}.
   *
   * @param scope the scope to address, or null for all scopes
   * @param kind the kind of metadata to select, or null for all metadata
   */
  Metadata getMetadata(MetadataEntity entity, @Nullable MetadataScope scope, @Nullable MetadataKind kind)
    throws IOException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link MetadataEntity} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   */
  Map<String, String> getProperties(MetadataEntity metadataEntity) throws IOException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link MetadataEntity} in the specified
   * {@link MetadataScope}
   */
  Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity) throws IOException;

  /**
   * @return all the tags for the specified {@link MetadataEntity} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   */
  Set<String> getTags(MetadataEntity metadataEntity) throws IOException;

  /**
   * @return all the tags for the specified {@link MetadataEntity} in the specified {@link MetadataScope}
   */
  Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) throws IOException;

  /**
   * Removes all the metadata (including properties and tags) for the specified {@link MetadataEntity}. This
   * API only supports removing metadata in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove metadata for
   */
  void removeMetadata(MetadataEntity metadataEntity, MutationOptions options) throws IOException;

  /**
   * Removes all properties from the metadata of the specified {@link MetadataEntity}. This API only supports
   * removing properties in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove properties for
   */
  void removeProperties(MetadataEntity metadataEntity, MutationOptions options) throws IOException;

  /**
   * Removes the specified keys from the metadata properties of the specified {@link MetadataEntity}. This API only
   * supports removing properties in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified properties for
   * @param keys the metadata property keys to remove
   */
  void removeProperties(MetadataEntity metadataEntity, Set<String> keys, MutationOptions options) throws IOException;

  /**
   * Removes all tags from the specified {@link MetadataEntity}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove tags for
   */
  void removeTags(MetadataEntity metadataEntity, MutationOptions options) throws IOException;

  /**
   * Removes the specified tags from the specified {@link MetadataEntity}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified tags for
   * @param tags the tags to remove
   */
  void removeTags(MetadataEntity metadataEntity, Set<String> tags, MutationOptions options) throws IOException;

  /**
   * Applies a metadata mutation directly.
   *
   * @param mutation the MetadataMutation to apply
   */
  void applyMutation(MetadataMutation mutation, MutationOptions options) throws IOException;

  /**
   * Applies a batch of metadata mutations
   */
  void applyMutations(List<? extends MetadataMutation> mutations, MutationOptions options) throws IOException;

  /**
   * Executes a search for CDAP entities.
   *
   * @param request the search request
   * @return the {@link SearchResponse} containing the results for the request
   */
  SearchResponse search(SearchRequest request) throws Exception;
}
