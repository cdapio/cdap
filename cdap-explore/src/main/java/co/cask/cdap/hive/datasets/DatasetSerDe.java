/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * SerDe to serialize Dataset Objects. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class DatasetSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSerDe.class);

  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
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
//TODO:
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO: add real Dataset stats - CDAP-12
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    return null;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return null;
  }

}
