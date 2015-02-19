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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Map;

/**
 * DatasetDefinition for {@link ObjectMappedTableDataset}.
 */
public class ObjectMappedTableDefinition extends AbstractDatasetDefinition<ObjectMappedTable, DatasetAdmin> {

  private static final Gson GSON = new Gson();
  private static final String TABLE_NAME = "objects";
  private final DatasetDefinition<? extends Table, ?> tableDef;

  public ObjectMappedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    Map<String, String> props = properties.getProperties();
    Preconditions.checkArgument(props.containsKey(ObjectMappedTableProperties.TYPE));
    // schema can normally be derived from the type. However, we cannot use the Type in this method because
    // this is called by the system, where the Type is often not available. for example, if somebody creates
    // an ObjectMappedTable<Purchase> where Purchase is a class internal to their app.
    // we require schema here because we want to validate it to make sure it is supported.
    Preconditions.checkArgument(props.containsKey(ObjectMappedTableProperties.SCHEMA));
    try {
      Schema schema = ObjectMappedTableProperties.getSchema(props);
      validateSchema(schema);
      return DatasetSpecification.builder(instanceName, getName())
        .properties(properties.getProperties())
        .datasets(tableDef.configure(TABLE_NAME, properties))
        .build();
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not parse schema.", e);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException("Schema is of an unsupported type.", e);
    }
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(spec.getSpecification(TABLE_NAME), classLoader);
  }

  @Override
  public ObjectMappedTableDataset<?> getDataset(DatasetSpecification spec, Map<String, String> arguments,
                                                ClassLoader classLoader) throws IOException {
    DatasetSpecification tableSpec = spec.getSpecification(TABLE_NAME);
    Table table = tableDef.getDataset(tableSpec, arguments, classLoader);

    TypeRepresentation typeRep = GSON.fromJson(
      spec.getProperty(ObjectMappedTableProperties.TYPE), TypeRepresentation.class);
    Schema schema = Schema.parseJson(spec.getProperty(ObjectMappedTableProperties.SCHEMA));
    return new ObjectMappedTableDataset(spec.getName(), table, typeRep, schema, classLoader);
  }

  private void validateSchema(Schema schema) throws UnsupportedTypeException {
    Schema.Type type = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
    if (type != Schema.Type.RECORD) {
      throw new UnsupportedTypeException("Unsupported type " + type + ". Must be a record.");
    }
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!fieldType.isSimpleType()) {
        throw new UnsupportedTypeException(
          String.format("Field %s is of unsupported type %s." +
                          " Must be a simple type (boolean, int, long, float, double, string, bytes).",
                        field.getName(), fieldType.toString()));
      }
    }
  }
}
