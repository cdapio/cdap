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

package co.cask.cdap.hive.table;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SerDe to deserialize {@link Row Rows} from a {@link Table}. It MUST implement the deprecated SerDe interface instead
 * of extending the abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class TableSerDe implements SerDe {
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    ArrayList<String> columnNames = Lists.newArrayList(properties.getProperty(serdeConstants.LIST_COLUMNS).split(","));
    ArrayList<TypeInfo> columnTypes =
      TypeInfoUtils.getTypeInfosFromTypeString(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES));

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
    // should not be writing to tables through this
    throw new SerDeException("Table serialization through Hive is not supported.");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    // this should always contain a Row object
    ObjectWritable objectWritable = (ObjectWritable) writable;
    Row row = (Row) objectWritable.get();
    // This will be replaced with schema related logic soon.
    return Lists.newArrayList(row.getRow(), row.getColumns());
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }
}
