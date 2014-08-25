/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.Properties;

/**
 * SerDe to serialize Dataset Objects.
 */
public class DatasetSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSerDe.class);

  private Type recordType;

  @Override
  public void initialize(Configuration entries, Properties properties) throws SerDeException {
    String datasetName = properties.getProperty(Constants.Explore.DATASET_NAME);

    // When initialize is called to write to a table, entries is null
    try {
      if (entries != null) {
        entries.set(Constants.Explore.DATASET_NAME, datasetName);
        recordType = DatasetAccessor.getRecordScannableType(entries);
      } else {
        recordType = DatasetAccessor.getRecordWritableType(null, datasetName);
      }
    } catch (IOException e) {
      LOG.error("Got exception while trying to instantiate dataset {}", datasetName, e);
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    // TODO make sure of that
    return ObjectWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    // NOTE: the object inspector here is not one that we build. It's a default one that Hive built,
    // that contains generic names for columns. The object is a list of objects, each element
    // representing one attribute of the Record type.
    // The object and the objectInspector represent one row of a query result to write into a dataset.
    // Therefore, it is not guaranteed that the object exactly matches the schema of the dataset
    // we want to write into.
    // TODO make sure o is a list of objects, even when the output of the query only has one column
    Object [] objects = (Object []) o;
    Class<?> [] classes = new Class[objects.length];
    for (int i = 0; i < objects.length; i++) {
      classes[i] = objects[i].getClass();
    }
    if (!(recordType instanceof Class)) {
      throw new RuntimeException(recordType + " should be a class");
    }
    Class<?> recordClass = (Class<?>) recordType;
    try {
      Constructor<?> constructor = recordClass.getConstructor(classes);
      return new ObjectWritable(constructor.newInstance(objects));
    } catch (Throwable e) {
      // TODO log the right exception message, saying the constructor should exist
      throw new SerDeException(e);
    }
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO: add real Sataset stats - REACTOR-278
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    ObjectWritable objectWritable = (ObjectWritable) writable;
    return objectWritable.get();
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return ObjectInspectorFactory.getReflectionObjectInspector(recordType);
  }
}
