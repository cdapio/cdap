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
   * The name of the key to use when exploring the table. Defaults to "key".
   */
  public static final String EXPLORE_KEY_NAME = "explore.key.name";

  /**
   * The type of the key when exploring the table. Defaults to bytes.
   */
  public static final String EXPLORE_KEY_TYPE = "explore.key.type";

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
  public static String getExploreKeyName(Map<String, String> properties) {
    return properties.get(EXPLORE_KEY_NAME);
  }

  /**
   * @return The type of the key when exploring the table.
   */
  public static Schema.Type getExploreKeyType(Map<String, String> properties) {
    return Schema.Type.valueOf(properties.get(EXPLORE_KEY_TYPE));
  }

  /**
   * A Builder to construct properties for {@link ObjectMappedTable} datasets.
   */
  public static class Builder extends DatasetProperties.Builder {

    /**
     * Package visible default constructor, to allow sub-classing by other datasets in this package.
     */
    Builder() {
      add(EXPLORE_KEY_NAME, "key");
      add(EXPLORE_KEY_TYPE, Schema.Type.BYTES.name());
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
     * Sets the name for the key to use when exploring the table. If no name is set it defaults to "key".
     */
    public Builder setExploreKeyName(String name) {
      add(EXPLORE_KEY_NAME, name);
      return this;
    }

    /**
     * Sets the type of the key to use when exploring the table. Currently only {@link Schema.Type#BYTES} and
     * {@link Schema.Type#STRING} are allowed. If no type is set it defaults to bytes.
     */
    public Builder setExploreKeyType(Schema.Type type) {
      Preconditions.checkArgument(type == Schema.Type.BYTES || type == Schema.Type.STRING,
                                  "Key type must be bytes or string.");
      add(EXPLORE_KEY_TYPE, type.name());
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
