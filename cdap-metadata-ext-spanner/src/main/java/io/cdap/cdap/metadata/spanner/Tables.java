/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.metadata.spanner;

/**
 * Defines Spanner table and column name constants for managing metadata.
 */
public class Tables {

  /**
   * Constants for the main 'metadata' table, which stores the core metadata record for each entity.
   */
  public static class Metadata {
    public static final String METADATA_ID_FIELD = "metadata_id";
    public static final String METADATA_COLUMN_FIELD = "metadata_column";
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "entity_type";
    public static final String NAME_FIELD = "name";
    public static final String VERSION = "version";
    public static final String CREATED_FIELD = "create_time";
    public static final String USER_FIELD = "user";
    public static final String SYSTEM_FIELD = "system";
  }

  /**
   * Constants for the 'metadata_props' table, a derived table used for indexing and querying
   * individual metadata properties and tags.
   */
  public static class MetadataProps {
    public static final String METADATA_ID_FIELD = "metadata_id";
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "entity_type";
    public static final String NESTED_NAME_FIELD = "name";
    public static final String NESTED_SCOPE_FIELD = "scope";
    public static final String NESTED_VALUE_FIELD = "value";
  }
}
