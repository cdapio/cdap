/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.data.schema.Schema;
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
import javax.annotation.Nullable;

/**
 * SerDe to serialize Dataset Objects. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class WranglerSerde implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(WranglerSerde.class);

  private ObjectInspector objectInspector;
  private ObjectDeserializer deserializer;
  private ObjectSerializer serializer;
  private Schema schema;

  @Override
  public void initialize(@Nullable Configuration conf, Properties properties) throws SerDeException {
    try {
      schema = Schema.parseJson(conf.get("output.schema"));
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    this.deserializer = new ObjectDeserializer(properties, schema);
    ArrayList<String> columnNames = Lists.newArrayList(StringUtils.split(properties.getProperty("columns"), ","));
    this.serializer = new ObjectSerializer(columnNames);
    this.objectInspector = deserializer.getInspector();
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new SerDeException("Trying to serialize with unknown object inspector type " +
                                 objectInspector.getClass().getName() + ". Expected StructObjectInspector.");
    }

    return serializer.serialize(obj, objectInspector);
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
