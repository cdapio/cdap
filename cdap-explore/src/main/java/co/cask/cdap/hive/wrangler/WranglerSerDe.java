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

package co.cask.cdap.hive.wrangler;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.hive.serde.ObjectDeserializer;
import co.cask.cdap.hive.serde.ObjectSerializer;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 *
 */
public class WranglerSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(WranglerSerDe.class);

  private ObjectInspector objectInspector;
  private ObjectDeserializer deserializer;
  private ObjectSerializer serializer;
  private Schema schema;
  private SerDeStats stats;

  @Override
  public void initialize(@Nullable Configuration conf, Properties properties) throws SerDeException {
    try {
      schema = Schema.parseJson(properties.getProperty("wrangler.explore.output.schema"));
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    LOG.info("******** schema is: {}", schema.toString());

    this.deserializer = new ObjectDeserializer(properties, schema);
    this.objectInspector = deserializer.getInspector();
    ArrayList<String> columnNames = Lists.newArrayList(StringUtils.split(properties.getProperty("columns"), ","));
    this.serializer = new ObjectSerializer(columnNames);
    this.stats = new SerDeStats();
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
    return stats;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    LOG.info("******* Deserializing the writable");
    StructuredRecordWritable structuredRecordWritable = (StructuredRecordWritable) writable;
    Object obj = structuredRecordWritable.get();
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
