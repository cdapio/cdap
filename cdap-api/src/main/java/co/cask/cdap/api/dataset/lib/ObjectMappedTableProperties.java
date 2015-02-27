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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Helper to build properties for an {@link ObjectMappedTable}.
 */
@Beta
public class ObjectMappedTableProperties {
  private static final SchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();
  private static final Gson GSON = new Gson();

  /**
   * The type of object in the table.
   */
  public static final String OBJECT_TYPE = "object.type";

  /**
   * The schema of objects in the table.
   */
  public static final String OBJECT_SCHEMA = "object.schema";

  /**
   * The name of the row key to use when exploring the table. Defaults to "key". This table column is the key used
   * by the {@link ObjectMappedTable#read(byte[])}, {@link ObjectMappedTable#write(byte[], Object)}, and
   * {@link ObjectMappedTable#delete(byte[])} methods.
   */
  public static final String ROW_KEY_EXPLORE_NAME = "row.key.explore.name";

  /**
   * The type of the row key when exploring the table. Defaults to bytes.
   */
  public static final String ROW_KEY_EXPLORE_TYPE = "row.key.explore.type";

  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The serialized representation of the type of objects in the table.
   */
  public static String getObjectTypeRepresentation(Map<String, String> properties) {
    return properties.get(OBJECT_TYPE);
  }

  /**
   * @return The schema of objects in the table.
   */
  public static Schema getObjectSchema(Map<String, String> properties) throws IOException {
    return Schema.parseJson(properties.get(OBJECT_SCHEMA));
  }

  /**
   * @return The name of the key to use when exploring the table.
   */
  public static String getRowKeyExploreName(Map<String, String> properties) {
    return properties.get(ROW_KEY_EXPLORE_NAME);
  }

  /**
   * @return The type of the key when exploring the table.
   */
  public static Schema.Type getRowKeyExploreType(Map<String, String> properties) {
    return Schema.Type.valueOf(properties.get(ROW_KEY_EXPLORE_TYPE));
  }

  /**
   * A Builder to construct properties for {@link ObjectMappedTable} datasets.
   */
  public static class Builder extends DatasetProperties.Builder {

    /**
     * Package visible default constructor, to allow sub-classing by other datasets in this package.
     */
    Builder() {
      add(ROW_KEY_EXPLORE_NAME, "rowkey");
      add(ROW_KEY_EXPLORE_TYPE, Schema.Type.BYTES.name());
    }

    /**
     * Sets the type of object in the table.
     */
    public Builder setType(Type type) throws UnsupportedTypeException {
      add(OBJECT_TYPE, GSON.toJson(new TypeRepresentation(type)));
      add(OBJECT_SCHEMA, schemaGenerator.generate(type, false).toString());
      return this;
    }

    /**
     * Sets the name for the key to use when exploring the table. If no name is set it defaults to "rowkey".
     * This is will be the name of the Hive column that will be mapped to the row key of Objects stored in the Table.
     * The key name must not also be a field name of the object stored in the Table.
     * For example, if your object contains a field named "rowkey", you will need to set the row key's
     * explore name to something other than "rowkey".
     */
    public Builder setRowKeyExploreName(String name) {
      add(ROW_KEY_EXPLORE_NAME, name);
      return this;
    }

    /**
     * Sets the type of the row key to use when exploring the table. Currently only {@link Schema.Type#BYTES} and
     * {@link Schema.Type#STRING} are allowed. If no type is set it defaults to bytes.
     */
    public Builder setRowKeyExploreType(Schema.Type type) {
      Preconditions.checkArgument(type == Schema.Type.BYTES || type == Schema.Type.STRING,
                                  "Key type must be bytes or string.");
      add(ROW_KEY_EXPLORE_TYPE, type.name());
      return this;
    }

    /**
     * Create a DatasetProperties from this builder.
     */
    public DatasetProperties build() {
      return super.build();
    }
  }
}
