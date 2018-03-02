/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.api.metadata;

import java.util.Map;
import javax.ws.rs.NotFoundException;

/**
 * The context for emitting metadata from programs
 */
public interface MetadataWriterContext {

  /**
   * Adds the specified {@link Map} to the metadata of the specified {@link MetadataEntity metadataEntity}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) throws NotFoundException;

  /**
   * Adds the specified tags to specified {@link MetadataEntity}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  void addTags(MetadataEntity metadataEntity, String... tags) throws NotFoundException;

  /**
   * Removes all the user metadata (including properties and tags) for the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove user metadata for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeMetadata(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes all properties from the user metadata of the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove properties for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes the specified keys from the user metadata properties of the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified properties for
   * @param keys the metadata property keys to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(MetadataEntity metadataEntity, String... keys) throws NotFoundException;

  /**
   * Removes all user tags from the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove tags for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes the specified user tags from the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified tags for
   * @param tags the tags to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(MetadataEntity metadataEntity, String... tags) throws NotFoundException;
}
