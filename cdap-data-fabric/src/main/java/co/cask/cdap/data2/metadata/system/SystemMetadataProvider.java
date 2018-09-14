/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Provides metadata at system scope for a {@link MetadataEntity}.
 */
public interface SystemMetadataProvider {

  String SCHEMA_KEY = "schema";
  String TTL_KEY = "ttl";
  String DESCRIPTION_KEY = "description";
  String ENTITY_NAME_KEY = "entity-name";
  String CREATION_TIME_KEY = "creation-time";
  String VERSION_KEY = "version";
  String EXPLORE_TAG = "explore";

  /**
   * Define the {@link MetadataScope#SYSTEM system} metadata properties to add for this entity.
   */
  Map<String, String> getSystemPropertiesToAdd();

  /**
   * Define the {@link MetadataScope#SYSTEM system} metadata tags to add for this entity.
   */
  default Set<String> getSystemTagsToAdd() {
    return Collections.emptySet();
  }

  /**
   * Define the {@link MetadataScope#SYSTEM system} schema to add for this entity.
   */
  @Nullable
  default String getSchemaToAdd() {
    return null;
  }
}
