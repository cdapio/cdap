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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamInputFormatConfigurer;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SerDe to serialize Dataset Objects.
 */
public class StreamSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSerDe.class);

  private ArrayList<String> columnNames;
  private ArrayList<TypeInfo> columnTypes;
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    // The column names are saved as the given inspector to #serialize doesn't preserves them
    // - maybe because it's an external table
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

    // set mapreduce configuration settings required by the stream input format
    String streamName = properties.getProperty(Constants.Explore.STREAM_NAME);
    try {
      // first get the context we are in
      ContextManager.Context context = ContextManager.getContext(conf);
      // get the stream admin from the context, which will let us get stream information such as the path
      StreamAdmin streamAdmin = context.getStreamAdmin();
      StreamConfig streamConfig = streamAdmin.getConfig(streamName);
      Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                 StreamUtils.getGeneration(streamConfig));
      StreamInputFormatConfigurer.setTTL(conf, streamConfig.getTTL());
      StreamInputFormatConfigurer.setStreamPath(conf, streamPath.toURI());
    } catch (IOException e) {
      LOG.error("Could not get the job context.", e);
      throw new SerDeException(e);
    }
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
