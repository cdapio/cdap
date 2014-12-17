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
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SerDe to deserialize Stream Events. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class StreamSerDe implements SerDe {
  private ArrayList<String> columnNames;
  private ArrayList<TypeInfo> columnTypes;
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
    // we return a list of the fields instead of just the StreamEvent because Hive's reflection object inspector
    // only looks at declared fields for that object class, and since StreamEvent extends StreamEventData,
    // the body and headers would be filtered out.  It also does not know what to do with a ByteBuffer.
    StreamEvent streamEvent = (StreamEvent) objectWritable.get();
    // This will be replaced with schema related logic soon.  For now, convert the body to a string.
    return Lists.newArrayList(
      streamEvent.getTimestamp(),
      Bytes.toString(streamEvent.getBody()),
      streamEvent.getHeaders());
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }
}
