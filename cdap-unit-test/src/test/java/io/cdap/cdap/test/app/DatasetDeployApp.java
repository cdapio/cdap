/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.lib.AbstractDataset;
import io.cdap.cdap.api.dataset.lib.CompositeDatasetDefinition;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTableProperties;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;

import java.io.IOException;
import java.util.Map;

/**
 * An app that embeds a dataset type and creates a dataset of that type.
 */
public class DatasetDeployApp extends AbstractApplication {
  public static final String NAME = "DatasetDeployApp";
  public static final String DATASET_NAME = "dataset";
  private static final Gson GSON = new Gson();

  public static class RecordConfig extends Config {
    private String className;

    public RecordConfig(String className) {
      this.className = className;
    }

    public String getClassName() {
      return className;
    }
  }

  @Override
  public void configure() {
    setName(NAME);
    addDatasetModule("record", RecordDatasetModule.class);
    createDataset(DATASET_NAME, RecordDataset.class.getName(),
                  DatasetProperties.builder().add("recordClassName", getRecordClass().getName()).build());
    addService("NoOpService", new NoOpHandler());
  }

  public static class RecordDatasetModule implements DatasetModule {

    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef = registry.get(KeyValueTable.class.getName());
      registry.add(new RecordDatasetDefinition(kvTableDef));
    }
  }

  static class RecordDatasetDefinition extends CompositeDatasetDefinition<RecordDataset> {

    RecordDatasetDefinition(DatasetDefinition<KeyValueTable, DatasetAdmin> kvTableDef) {
      super(RecordDataset.class.getName(), ImmutableMap.of("kv", kvTableDef));
    }

    @Override
    public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
      String schemaString = getSchemaString(properties);
      return DatasetSpecification.builder(instanceName, RecordDataset.class.getName())
        .properties(properties.getProperties())
        .property("schema", schemaString)
        .datasets(getDelegate("kv").configure("kv", DatasetProperties.EMPTY))
        .build();
    }

    @Override
    public DatasetSpecification reconfigure(String instanceName,
                                            DatasetProperties newProperties,
                                            DatasetSpecification currentSpec) throws IncompatibleUpdateException {
      if (!currentSpec.getProperties().get("schema").equals(getSchemaString(newProperties))) {
        throw new IncompatibleUpdateException("Attempt to alter schema");
      }
      return configure(instanceName, newProperties);
    }

    private String getSchemaString(DatasetProperties properties) {
      try {
        String className = properties.getProperties().get("recordClassName");
        Class<?> recordClass = Class.forName(className);
        Schema schema = ObjectMappedTableProperties.getObjectSchema(
          ObjectMappedTableProperties.builder().setType(recordClass).build().getProperties());
        schema = Schema.recordOf("record", schema.getFields());
        return schema.toString();
      } catch (ClassNotFoundException | UnsupportedTypeException | IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public RecordDataset getDataset(DatasetContext datasetContext,
                                    DatasetSpecification spec,
                                    Map<String, String> arguments,
                                    ClassLoader classLoader) throws IOException {
      DatasetDefinition<KeyValueTable, DatasetAdmin> kvTbleDef = getDelegate("kv");
      KeyValueTable kvTable = kvTbleDef.getDataset(datasetContext, spec.getSpecification("kv"), arguments, classLoader);
      try {
        return new RecordDataset(spec, kvTable);
      } catch (ClassNotFoundException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public static final class RecordDataset extends AbstractDataset {
    private final KeyValueTable table;
    private final Class recordClass;

    public RecordDataset(DatasetSpecification spec, KeyValueTable table) throws ClassNotFoundException {
      super(spec.getName(), table);
      this.table = table;
      this.recordClass = Class.forName(spec.getProperty("recordClassName"));
    }

    public Object getRecord(String key) {
      byte[] serializedRecord = table.read(key);
      if (serializedRecord == null) {
        return null;
      }
      return GSON.fromJson(Bytes.toString(table.read(key)), recordClass);
    }

    public void writeRecord(String key, Object object) {
      Preconditions.checkArgument(recordClass.isInstance(object));
      table.write(key, GSON.toJson(object, recordClass));
    }

    public String getRecordClassName() {
      return recordClass.getName();
    }
  }

  protected Class<?> getRecordClass() {
    return Record.class;
  }

  public static final class Record {
    private final String id;
    private final String firstName;
    private final String lastName;

    public Record(String id, String firstName, String lastName) {
      this.id = id;
      this.firstName = firstName;
      this.lastName = lastName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Record that = (Record) o;

      return Objects.equal(this.id, that.id) &&
        Objects.equal(this.firstName, that.firstName) &&
        Objects.equal(this.lastName, that.lastName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, firstName, lastName);
    }
  }

  public static final class NoOpHandler extends AbstractHttpServiceHandler {

  }

}
