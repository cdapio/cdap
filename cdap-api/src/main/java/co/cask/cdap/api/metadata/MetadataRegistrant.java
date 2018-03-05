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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.RuntimeContext;

/**
 * Class which registers metadata for programs so
 * that they are available in {@link co.cask.cdap.api.ProgramLifecycle#initialize(RuntimeContext)} and
 * {@link ProgramLifecycle#destroy()}
 */
public interface MetadataRegistrant {

  /**
   * Register the {@link MetadataRecord} (including properties and tags) for the specified {@link MetadataEntity}
   * in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM} if the metadata entity exists and has any
   * metadata associated with it.
   */
  void registerMetadata(MetadataEntity metadataEntity);

  /**
   * Registers a {@link MetadataRecord} representing all metadata (including properties and tags) for the
   * specified {@link MetadataEntity} in the specified {@link MetadataScope} if the metadata entity exists and has any
   * metadata associated with it in the given scope.
   */
  void registerMetadata(MetadataScope scope, MetadataEntity metadataEntity);
}
