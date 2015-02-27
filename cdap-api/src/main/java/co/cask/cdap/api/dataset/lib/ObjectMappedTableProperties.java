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
   * The schema of objects in the table. This schema does not include the row key.
   */
  public static final String OBJECT_SCHEMA = "object.schema";

  /**
   * The name of the Hive table column for the key of objects stored in the table.
   * See {@link Builder#setRowKeyExploreName(String)} for more details.
   */
  public static final String ROW_KEY_EXPLORE_NAME = "row.key.explore.name";

  /**
   * The type of the Hive table column for the row key of objects stored in the table.
   * See {@link Builder#setRowKeyExploreType(Schema.Type)} for more details.
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
     * Sets the type of object stored in the table. The schema of the Hive table for an ObjectMappedTable
     * is derived from the object type set here and the row key explore name set by
     * {@link #setRowKeyExploreName(String)}.
     *
     * For example, if the type set here has three fields - "id", "name", and "price", the corresponding Hive table
     * for this Dataset will contain four columns - "rowkey", "id", "name", and "price".
     */
    public Builder setType(Type type) throws UnsupportedTypeException {
      add(OBJECT_TYPE, GSON.toJson(new TypeRepresentation(type)));
      add(OBJECT_SCHEMA, schemaGenerator.generate(type, false).toString());
      return this;
    }

    /**
     * Sets the row key column name in the corresponding Hive table for an ObjectMappedTable.
     * The schema of the Hive table for an ObjectMappedTable is derived from the object type set by
     * {@link #setType(Type)} and the row key explore name set here. The name set here cannot be the same
     * as any fields in the object type.
     *
     * For example, if you are storing an Object with a single string field named "id", the corresponding
     * Hive table will have a schema of (rowkey binary, id string). If you set the name of the row key to "name",
     * the corresponding Hive table will have a schema of (name binary, id string) instead of the default.
     */
    public Builder setRowKeyExploreName(String name) {
      add(ROW_KEY_EXPLORE_NAME, name);
      return this;
    }

    /**
     * Sets the column type for the row key column in the corresponding Hive table for an ObjectMappedTable.
     * By default, the type of the row key in your Hive table will be binary. You can set the type using this
     * method. Only {@link Schema.Type#BYTES} and {@link Schema.Type#STRING} are allowed.
     *
     * For example, if you are storing an Object with a single string field named "id", the corresponding
     * Hive table will have a schema of (rowkey binary, id string). If you set the type to a string using this method,
     * the corresponding Hive table will have a schema of (rowkey string, id string) instead of the default.
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
