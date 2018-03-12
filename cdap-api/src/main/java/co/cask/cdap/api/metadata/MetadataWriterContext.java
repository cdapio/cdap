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

/**
 * The context for emitting metadata from programs
 */
public interface MetadataWriterContext {

  /**
   * Adds the specified {@link Map} to the metadata of the specified {@link MetadataEntity metadataEntity}.
   * Existing keys will be updated with new values.
   */
  void addProperties(MetadataEntity metadataEntity, Map<String, String> properties);

  /**
   * Adds the specified tags to specified {@link MetadataEntity}. If a given tag already exists for the metadata
   * entity it will be skipped.
   */
  void addTags(MetadataEntity metadataEntity, String... tags);

  /**
   * Adds all the specified tags to specified {@link MetadataEntity}. If a given tag already exists for the metadata
   * entity it will be skipped.
   */
  void addTags(MetadataEntity metadataEntity, Iterable<String> tags);

  /**
   * Removes all the user metadata (including properties and tags) for the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove user metadata for
   */
  void removeMetadata(MetadataEntity metadataEntity);

  /**
   * Removes all properties from the user metadata of the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove properties for
   */
  void removeProperties(MetadataEntity metadataEntity);

  /**
   * Removes the specified keys from the user metadata properties of the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified properties for
   * @param keys the metadata property keys to remove
   */
  void removeProperties(MetadataEntity metadataEntity, String... keys);

  /**
   * Removes all user tags from the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove tags for
   */
  void removeTags(MetadataEntity metadataEntity);

  /**
   * Removes the specified user tags from the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified tags for
   * @param tags the tags to remove
   */
  void removeTags(MetadataEntity metadataEntity, String... tags);
}
