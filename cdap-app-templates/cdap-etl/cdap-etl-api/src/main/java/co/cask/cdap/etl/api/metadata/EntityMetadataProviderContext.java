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
package co.cask.cdap.etl.api.metadata;

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;

import java.util.Map;

/**
 * Interfaces for methods exposed in pipeline to read metadata for a particular entity.  This reader is tied to an
 * entity and can only read metadata specific to the entity.
 */
public interface EntityMetadataProviderContext {
  /**
   * Returns a Map of {@link MetadataScope} and {@link Metadata} representing all metadata
   * (including properties and tags) in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   */
  Map<MetadataScope, Metadata> getMetadata();

  /**
   * Returns a Map of {@link MetadataScope} and {@link Metadata} representing all metadata
   * in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} associated with the
   * {@link MetadataEntity} created by adding on subParts after the {@link MetadataEntity} associated with this
   * {@link EntityMetadataProviderContext}.
   */
  Map<MetadataScope, Metadata> getMetadata(MetadataEntity subParts);

  /**
   * @return all the {@link Metadata} (including properties and tags) associated with the {@link MetadataEntity}
   * in the given scope.
   */
  Metadata getMetadata(MetadataScope scope);

  /**
   * @return all the {@link Metadata} (including properties and tags) in the specified {@link MetadataScope}
   * associated with the {@link co.cask.cdap.api.metadata.MetadataEntity}
   * created by adding on subParts after the {@link co.cask.cdap.api.metadata.MetadataEntity} associated with this
   * {@link EntityMetadataProviderContext}.
   */
  Metadata getMetadata(MetadataScope scope, MetadataEntity subParts);
}
