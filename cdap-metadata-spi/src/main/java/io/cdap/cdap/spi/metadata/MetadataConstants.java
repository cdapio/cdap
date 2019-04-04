/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Defines implementation-independent constants for Metadata.
 */
@Beta
public class MetadataConstants {

  public static final String CREATION_TIME_KEY = "creation-time";
  public static final String DESCRIPTION_KEY = "description";
  public static final String ENTITY_NAME_KEY = "entity-name";
  public static final String PROPERTIES_KEY = "properties";
  public static final String SCHEMA_KEY = "schema";
  public static final String TAGS_KEY = "tags";
  public static final String TTL_KEY = "ttl";

  public static final String KEYVALUE_SEPARATOR = ":";
}
