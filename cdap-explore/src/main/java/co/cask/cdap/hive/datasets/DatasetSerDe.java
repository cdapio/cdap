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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.context.NullJobConfException;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveBaseCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyNonPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * SerDe to serialize Dataset Objects. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
public class DatasetSerDe implements SerDe {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DatasetSerDe.class);

  private ArrayList<String> columnNames;
  private Type recordType;

  @Override
  public void initialize(Configuration entries, Properties properties) throws SerDeException {
    // The column names are saved as the given inspector to #serialize doesn't preserves them
    // - maybe because it's an external table
    // The columns property comes from the Hive metastore, which has it from the create table statement
    // It is then important that this schema be accurate and in the right order - the same order as
    // object inspectors will reflect them.
    columnNames = new ArrayList<String>(Arrays.asList(StringUtils.split(properties.getProperty("columns"), ",")));

    String datasetName = properties.getProperty(Constants.Explore.DATASET_NAME);
    try {
      if (entries != null) {
        // Here, we can't say whether Hive wants to read the table, or write to it
        Configuration conf = new Configuration(entries);
        conf.set(Constants.Explore.DATASET_NAME, datasetName);
        recordType = DatasetAccessor.getRecordType(conf);
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

    //overwrite field names (as they get lost by Hive)
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector);
    structTypeInfo.setAllStructFieldNames(columnNames);

    List<TypeInfo> info = structTypeInfo.getAllStructFieldTypeInfos();
    List<String> names = structTypeInfo.getAllStructFieldNames();

    Map<String, Object> recordMap = Maps.newConcurrentMap();
    List<Object> recordObjects = ((StructObjectInspector) objectInspector).getStructFieldsDataAsList(o);

    for (int structIndex = 0; structIndex < info.size(); structIndex++) {
      Object obj = recordObjects.get(structIndex);
      if (obj instanceof LazyNonPrimitive || obj instanceof LazyPrimitive) {
        // In case the SerDe that deserialized the object is the one of a native table
        recordMap.put(names.get(structIndex), fromLazyObject(info.get(structIndex), obj));
      } else if (obj instanceof Writable) {
        // Native tables sometimes introduce primitive Writable objects at this point
        recordMap.put(names.get(structIndex), fromWritable((Writable) obj));
      } else {
        // In case the deserializer is the DatasetSerDe
        recordMap.put(names.get(structIndex), obj);
      }
    }

    // TODO Improve serialization logic - CDAP-11
    return new Text(GSON.toJson(recordMap));
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO: add real Sataset stats - CDAP-12
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

  private Object fromWritable(Writable writable) {
    if (writable instanceof IntWritable) {
      return ((IntWritable) writable).get();
    } else if (writable instanceof LongWritable) {
      return ((LongWritable) writable).get();
    } else if (writable instanceof ShortWritable) {
      return ((ShortWritable) writable).get();
    } else if (writable instanceof BooleanWritable) {
      return ((BooleanWritable) writable).get();
    } else if (writable instanceof DoubleWritable) {
      return ((DoubleWritable) writable).get();
    } else if (writable instanceof FloatWritable) {
      return ((FloatWritable) writable).get();
    } else if (writable instanceof Text) {
      return writable.toString();
    } else if (writable instanceof BytesWritable) {
      return ((BytesWritable) writable).getBytes();
    } else if (writable instanceof ByteWritable) {
      return ((ByteWritable) writable).get();
    } else if (writable instanceof DateWritable) {
      return ((DateWritable) writable).get();
    } else if (writable instanceof org.apache.hadoop.hive.serde2.io.ShortWritable) {
      return ((org.apache.hadoop.hive.serde2.io.ShortWritable) writable).get();
    } else if (writable instanceof HiveBaseCharWritable) {
      return ((HiveBaseCharWritable) writable).getTextValue().toString();
    } else if (writable instanceof TimestampWritable) {
      return ((TimestampWritable) writable).getTimestamp();
    } else if (writable instanceof org.apache.hadoop.hive.serde2.io.DoubleWritable) {
      return ((org.apache.hadoop.hive.serde2.io.DoubleWritable) writable).get();
    } else if (writable instanceof HiveDecimalWritable) {
      return ((HiveDecimalWritable) writable).getHiveDecimal();
    } else if (writable instanceof NullWritable) {
      return null;
    }
    return writable.toString();
  }

  private Object fromLazyObject(TypeInfo type, Object data) {
    if (data == null) {
      return null;
    }

    switch (type.getCategory()) {
      case PRIMITIVE:
        Writable writable = ((LazyPrimitive) data).getWritableObject();
        return fromWritable(writable);

      case LIST:
        ListTypeInfo listType = (ListTypeInfo) type;
        TypeInfo listElementType = listType.getListElementTypeInfo();

        List<Object> list = ((LazyArray) data).getList();
        if (list.isEmpty()) {
          return ImmutableList.of();
        }

        Object[] arrayContent = new Object[list.size()];
        for (int i = 0; i < arrayContent.length; i++) {
          arrayContent[i] = fromLazyObject(listElementType, list.get(i));
        }
        return arrayContent;

      case MAP:
        MapTypeInfo mapType = (MapTypeInfo) type;

        Map<Object, Object> mapContent = Maps.newConcurrentMap();
        Map<Object, Object> map = ((LazyMap) data).getMap();

        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          mapContent.put(fromLazyObject(mapType.getMapKeyTypeInfo(), entry.getKey()),
                         fromLazyObject(mapType.getMapValueTypeInfo(), entry.getValue()));
        }
        return mapContent;

      case STRUCT:
        StructTypeInfo structType = (StructTypeInfo) type;
        List<TypeInfo> info = structType.getAllStructFieldTypeInfos();
        List<String> names = structType.getAllStructFieldNames();

        Map<String, Object> structMap = Maps.newConcurrentMap();
        List<Object> struct = ((LazyStruct) data).getFieldsAsList();

        for (int structIndex = 0; structIndex < info.size(); structIndex++) {
          structMap.put(names.get(structIndex),
                        fromLazyObject(info.get(structIndex), struct.get(structIndex)));
        }
        return structMap;
      case UNION:
        throw new UnsupportedOperationException("union not yet supported");

      default:
        return data.toString();
    }
  }
}
