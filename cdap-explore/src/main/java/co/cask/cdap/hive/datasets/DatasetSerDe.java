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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.context.NullJobConfException;
import co.cask.cdap.hive.serde.ObjectDeserializer;
import co.cask.cdap.hive.serde.ObjectSerializer;
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
import java.util.ArrayList;
import java.util.Properties;

/**
 * SerDe to serialize Dataset Objects. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class DatasetSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSerDe.class);

  private ObjectInspector objectInspector;
  private ObjectDeserializer deserializer;
  private ObjectSerializer serializer;

  @Override
  public void initialize(Configuration entries, Properties properties) throws SerDeException {
    // The column names are saved as the given inspector to #serialize doesn't preserves them
    // - maybe because it's an external table
    // The columns property comes from the Hive metastore, which has it from the create table statement
    // It is then important that this schema be accurate and in the right order - the same order as
    // object inspectors will reflect them.
    String datasetName = properties.getProperty(Constants.Explore.DATASET_NAME);
    String namespace = properties.getProperty(Constants.Explore.DATASET_NAMESPACE);

    // no namespace SHOULD be an exception but... Hive calls initialize in several places, one of which is
    // when you try and drop a table.
    // When updating to CDAP 2.8, old tables will not have namespace as a serde property. So in order
    // to avoid a null pointer exception that prevents dropping a table, we handle the null namespace case here.
    if (namespace == null) {
      // we also still need an ObjectInspector as Hive uses it to check what columns the table has.
      this.objectInspector = new ObjectDeserializer(properties, null).getInspector();
      return;
    }

    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, datasetName);
    try {
      Schema schema = DatasetAccessor.getRecordSchema(entries, datasetInstanceId);
      this.deserializer = new ObjectDeserializer(properties, schema);
      ArrayList<String> columnNames = Lists.newArrayList(StringUtils.split(properties.getProperty("columns"), ","));
      this.serializer = new ObjectSerializer(columnNames);
      this.objectInspector = deserializer.getInspector();
    } catch (NullJobConfException e) {
      // This is the case when this serDe is used by Hive only for its serialize method. In that case,
      // We don't need to initialize a context since serialize does not need any dataset information.
      LOG.warn("Could not initialize record writable dataset. Carrying on.");
    } catch (IOException e) {
      LOG.error("Got exception while trying to instantiate dataset {}", datasetName, e);
      throw new SerDeException(e);
    } catch (UnsupportedTypeException e) {
      LOG.error("Unsupported schema {}");
      throw new SerDeException("Schema is of an unsupported type.");
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
