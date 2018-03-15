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
package co.cask.cdap.api.metadata.plugin;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataRecord;
import co.cask.cdap.api.metadata.MetadataScope;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interfaces for methods exposed in pipeline to read metadata for a particular entity.  This reader is tied to an
 * entity and can only read metadata specific to the entity.
 */
public interface EntityMetadataReaderContext {
  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags)
   * in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Set<MetadataRecord> getMetadata();

  /**
   * @return all the tags associated with the {@link co.cask.cdap.api.metadata.MetadataEntity} created by calling
   * {@link co.cask.cdap.api.metadata.MetadataEntity#append(String, String)} on subParts to the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this {@link EntityMetadataReaderContext}. The
   * subParts should be a strings representing the key values to be added.
   */
  Set<MetadataRecord> getMetadata(List<MetadataEntity.KeyValue> subParts);

  /**
   * @return a set of {@link MetadataRecord} representing all metadata (including properties and tags)
   * in the specified {@link MetadataScope}.
   */
  Set<MetadataRecord> getMetadata(MetadataScope scope);

  /**
   * @return a set of {@link MetadataRecord} representing all metadata (including properties and tags)
   * in the specified {@link MetadataScope}. associated with the {@link co.cask.cdap.api.metadata.MetadataEntity}
   * created by calling {@link co.cask.cdap.api.metadata.MetadataEntity#append(String, String)} on subParts to the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this {@link EntityMetadataReaderContext}. The
   * subParts should be a strings representing the key values to be added.
   */
  Set<MetadataRecord> getMetadata(MetadataScope scope, List<MetadataEntity.KeyValue> subParts);

  /**
   * @return all the properties in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   */
  Map<MetadataScope, Map<String, String>> getProperties();

  /**
   * @return all the properties in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} associated with the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} created by calling
   * {@link co.cask.cdap.api.metadata.MetadataEntity#append(String, String)} on subParts to the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this {@link EntityMetadataReaderContext}. The
   * subParts should be a strings representing the key values to be added.
   */
  Map<MetadataScope, Map<String, String>> getProperties(List<MetadataEntity.KeyValue> subParts);

  /**
   * @return a {@link Map} representing the metadata in the specified {@link MetadataScope}
   */
  Map<String, String> getProperties(MetadataScope scope);

  /**
   * @return a {@link Map} representing the metadata in the specified {@link MetadataScope} associated with the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} created by calling
   * {@link co.cask.cdap.api.metadata.MetadataEntity#append(String, String)} on subParts to the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this {@link EntityMetadataReaderContext}. The
   * subParts should be a strings representing the key values to be added.
   */
  Map<String, String> getProperties(MetadataScope scope, List<MetadataEntity.KeyValue> subParts);

  /**
   * @return all the tags in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   */
  Map<MetadataScope, Set<String>> getTags();

  /**
   * @return all the tags in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} associated with the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} created by calling
   * {@link co.cask.cdap.api.metadata.MetadataEntity#append(String, String)} on subParts to the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this {@link EntityMetadataReaderContext}. The
   * subParts should be a strings representing the key values to be added.
   */
  Map<MetadataScope, Set<String>> getTags(List<MetadataEntity.KeyValue> subParts);

  /**
   * @return all the tags in the specified {@link MetadataScope}
   */
  Set<String> getTags(MetadataScope scope);

  /**
   * @return all the tags associated with the {@link co.cask.cdap.api.metadata.MetadataEntity} created by calling
   * {@link co.cask.cdap.api.metadata.MetadataEntity#append(String, String)} on subParts to the
   * {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this {@link EntityMetadataReaderContext}. The
   * subParts should be a strings representing the key values to be added.
   */
  Set<String> getTags(MetadataScope scope, List<MetadataEntity.KeyValue> subParts);
}
