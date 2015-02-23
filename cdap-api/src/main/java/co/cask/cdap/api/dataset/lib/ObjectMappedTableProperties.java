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
  public static final String TYPE = "type";

  /**
   * The schema of objects in the table.
   */
  public static final String SCHEMA = "schema";

  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The schema of objects in the table.
   */
  public static Schema getSchema(Map<String, String> properties) throws IOException {
    return Schema.parseJson(properties.get(SCHEMA));
  }

  /**
   * A Builder to construct properties for {@link ObjectMappedTable} datasets.
   */
  public static class Builder extends DatasetProperties.Builder {

    /**
     * Package visible default constructor, to allow sub-classing by other datasets in this package.
     */
    Builder() { }

    /**
     * Sets the type of object in the table.
     */
    public Builder setType(Type type) throws UnsupportedTypeException {
      add(TYPE, GSON.toJson(new TypeRepresentation(type)));
      add(SCHEMA, schemaGenerator.generate(type, false).toString());
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
