/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.serde.ObjectDeserializer;
import co.cask.cdap.hive.serde.ObjectSerializer;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Properties;

/**
 * SerDe to serialize Dataset Objects. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class DatasetSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSerDe.class);
  private static final SchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();

  private ObjectInspector objectInspector;
  private ObjectDeserializer deserializer;
  private ObjectSerializer serializer;
  private Schema schema;

  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    // The column names are saved as the given inspector to #serialize doesn't preserves them
    // - maybe because it's an external table
    // The columns property comes from the Hive metastore, which has it from the create table statement
    // It is then important that this schema be accurate and in the right order - the same order as
    // object inspectors will reflect them.
    String datasetName = properties.getProperty(Constants.Explore.DATASET_NAME);
    String namespace = properties.getProperty(Constants.Explore.DATASET_NAMESPACE);

    // no namespace SHOULD be an exception but... Hive calls initialize in several places, one of which is
    // when you try and drop a table.
    // When updating from CDAP 2.6, old tables will not have namespace as a serde property. So in order
    // to avoid a null pointer exception that prevents dropping a table, we handle the null namespace case here.
    if (namespace == null) {
      // we also still need an ObjectInspector as Hive uses it to check what columns the table has.
      this.objectInspector = new ObjectDeserializer(properties, null).getInspector();
      return;
    }

    if (datasetName == null || datasetName.isEmpty()) {
      throw new SerDeException("Dataset name not found in serde properties.");
    }

    // Hive may call initialize a bunch of times... so remember the schema so we don't instantiate the dataset
    // a bunch of times.
    if (schema == null) {
      Id.DatasetInstance datasetId = Id.DatasetInstance.from(namespace, datasetName);
      getDatasetSchema(conf, datasetId);
    }

    this.deserializer = new ObjectDeserializer(properties, schema);
    ArrayList<String> columnNames = Lists.newArrayList(StringUtils.split(properties.getProperty("columns"), ","));
    this.serializer = new ObjectSerializer(columnNames);
    this.objectInspector = deserializer.getInspector();
  }

  private void getDatasetSchema(Configuration conf, Id.DatasetInstance datasetId) throws SerDeException {

    try (ContextManager.Context hiveContext = ContextManager.getContext(conf)) {

      // some datasets like Table and ObjectMappedTable have schema in the dataset properties
      try {
        DatasetSpecification datasetSpec = hiveContext.getDatasetSpec(datasetId);
        String schemaStr = datasetSpec.getProperty("schema");
        if (schemaStr != null) {
          schema = Schema.parseJson(schemaStr);
          return;
        }
      } catch (DatasetManagementException e) {
        throw new SerDeException("Could not instantiate dataset " + datasetId, e);
      } catch (IOException e) {
        throw new SerDeException("Exception getting schema for dataset " + datasetId, e);
      }

      // other datasets must be instantiated to get their schema
      // conf is null if this is a query that writes to a dataset
      ClassLoader parentClassLoader = conf == null ? null : conf.getClassLoader();
      try (SystemDatasetInstantiator datasetInstantiator = hiveContext.createDatasetInstantiator(parentClassLoader)) {
        Dataset dataset = datasetInstantiator.getDataset(datasetId);
        if (dataset == null) {
          throw new SerDeException("Could not find dataset " + datasetId);
        }
        Type recordType;
        if (dataset instanceof RecordScannable) {
          recordType = ((RecordScannable) dataset).getRecordType();
        } else if (dataset instanceof RecordWritable) {
          recordType = ((RecordWritable) dataset).getRecordType();
        } else {
          throw new SerDeException("Dataset " + datasetId + " is not explorable.");
        }
        schema = schemaGenerator.generate(recordType);
      } catch (UnsupportedTypeException e) {
        throw new SerDeException("Dataset " + datasetId + " has an unsupported schema.", e);
      } catch (IOException e) {
        throw new SerDeException("Exception while trying to instantiate dataset " + datasetId, e);
      }
    } catch (IOException e) {
      throw new SerDeException("Could not get hive context from configuration.", e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    // NOTE: the object inspector here is not one that we build. It's a default one that Hive built,
    // that contains generic names for columns. The object is a list of objects, each element
    // representing one attribute of the Record type.
    // The object and the objectInspector represent one row of a query result to write into a dataset.
    // Therefore, it is not guaranteed that the object exactly matches the schema of the dataset
    // we want to write into.

    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new SerDeException("Trying to serialize with unknown object inspector type " +
                                 objectInspector.getClass().getName() + ". Expected StructObjectInspector.");
    }

    return serializer.serialize(o, objectInspector);
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO: add real Dataset stats - CDAP-12
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    ObjectWritable objectWritable = (ObjectWritable) writable;
    Object obj = objectWritable.get();
    try {
      return deserializer.deserialize(obj);
    } catch (Throwable t) {
      LOG.error("Unable to deserialize object {}.", obj, t);
      throw new SerDeException("Unable to deserialize an object.", t);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

}
