/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.hive.context.NullJobConfException;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    try {
      if (entries != null) {
        entries.set(Constants.Explore.DATASET_NAME, datasetName);
        recordType = DatasetAccessor.getRecordScannableType(entries);
      } else {
        // When initialize is called to write to a table, entries is null
        try {
          recordType = DatasetAccessor.getRecordWritableType(datasetName);
        } catch (NullJobConfException e) {
          // This is the case when this serDe is used by Hive only for its serialize method. In that case,
          // We don't need to initialize a context since serialize does not need any dataset information.
          LOG.warn("Could not initialize record writable dataset. Carrying on.");
          recordType = null;
        }
      }
    } catch (IOException e) {
      LOG.error("Got exception while trying to instantiate dataset {}", datasetName, e);
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
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
    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new SerDeException("Trying to serialize with unknown object inspector type " +
                                 objectInspector.getClass().getName() + ". Expected StructObjectInspector.");
    }
    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    return new ObjectWritable(soi.getStructFieldsDataAsList(o));
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
    Preconditions.checkNotNull(recordType, "Record type should not be null.");
    return ObjectInspectorFactory.getReflectionObjectInspector(recordType);
  }
}
