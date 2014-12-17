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

package co.cask.cdap.hive.stream;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import co.cask.cdap.hive.serde.ObjectTranslator;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SerDe to deserialize Stream Events. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class StreamSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSerDe.class);
  private ArrayList<String> columnNames;
  private ArrayList<TypeInfo> columnTypes;
  private List<String> bodyColumnNames;
  private List<TypeInfo> bodyColumnTypes;
  private ObjectInspector inspector;

  // initialize gets called multiple times by Hive. It may seem like a good idea to put additional settings into
  // the conf, but be very careful when doing so. If there are multiple hive tables involved in a query, initialize
  // for each table is called before input splits are fetched for any table. It is therefore not safe to put anything
  // the input format may need into conf in this method. Rather, use StorageHandler's method to place needed config
  // into the properties map there, which will get passed here and also copied into the job conf for the input
  // format to consume.
  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    // The column names are saved as the given inspector to #serialize doesn't preserves them
    // - maybe because it's an external tTable
    // The columns property comes from the Hive metastore, which has it from the create table statement
    // It is then important that this schema be accurate and in the right order - the same order as
    // object inspectors will reflect them.
    columnNames = Lists.newArrayList(properties.getProperty(serdeConstants.LIST_COLUMNS).split(","));
    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES));
    bodyColumnNames = columnNames.subList(2, columnNames.size());
    bodyColumnTypes = columnTypes.subList(2, columnTypes.size());

    int numCols = columnNames.size();

    final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);

    for (int i = 0; i < numCols; i++) {
      columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(i)));
    }

    this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    // should not be writing to streams through this
    throw new SerDeException("Stream serialization through Hive is not supported.");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    // this should always contain a StreamEvent object
    ObjectWritable objectWritable = (ObjectWritable) writable;
    StreamEvent streamEvent = (StreamEvent) objectWritable.get();

    // timestamp and headers are always guaranteed to be first.
    List<Object> event = Lists.newArrayList();
    event.add(streamEvent.getTimestamp());
    event.add(streamEvent.getHeaders());

    try {
      // TODO: replace with conversion to a format specific object. The body should always be a record.
      StreamBody formattedBody = new StreamBody(streamEvent.getBody());
      event.addAll(ObjectTranslator.flattenRecord(formattedBody, bodyColumnNames, bodyColumnTypes));
      return event;
    } catch (Throwable t) {
      LOG.info("Unable to format the stream body.", t);
      throw new SerDeException("Unable to format the stream body.", t);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  // TODO: temporary class that will be removed once format logic is in place.
  private static class StreamBody {
    private final String body;

    private StreamBody(ByteBuffer bodyBytes) {
      this.body = Bytes.toString(bodyBytes);
    }
  }
}
