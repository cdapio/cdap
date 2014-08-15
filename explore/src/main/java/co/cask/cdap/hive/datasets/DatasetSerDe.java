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
    entries.set(Constants.Explore.DATASET_NAME, datasetName);
    try {
      recordType = DatasetAccessor.getRecordScannableType(entries);
    } catch (IOException e) {
      LOG.error("Got exception while trying to instantiate dataset {}", datasetName, e);
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    // Writing to Datasets not supported.
    return Writable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    throw new UnsupportedOperationException("Cannot write to Datasets yet");
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
