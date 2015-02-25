/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.hive.serde;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
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
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Used to serialize objects in a SerDe. Objects can come from native Hive tables or they can come from the
 * {@link ObjectDeserializer}.
 */
public class ObjectSerializer {
  private static final Gson GSON = new Gson();
  private final ArrayList<String> columnNames;

  public ObjectSerializer(ArrayList<String> columnNames) {
    this.columnNames = columnNames;
  }

  public Writable serialize(Object o, ObjectInspector objectInspector) {
    //overwrite field names (as they get lost by Hive)
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector);
    structTypeInfo.setAllStructFieldNames(columnNames);

    List<TypeInfo> info = structTypeInfo.getAllStructFieldTypeInfos();
    List<String> names = structTypeInfo.getAllStructFieldNames();

    Map<String, Object> recordMap = Maps.newConcurrentMap();
    List<Object> recordObjects = ((StructObjectInspector) objectInspector).getStructFieldsDataAsList(o);

    for (int structIndex = 0; structIndex < info.size(); structIndex++) {
      Object obj = recordObjects.get(structIndex);
      TypeInfo objType = info.get(structIndex);
      if (obj instanceof LazyNonPrimitive || obj instanceof LazyPrimitive) {
        // In case the SerDe that deserialized the object is the one of a native table
        recordMap.put(names.get(structIndex), fromLazyObject(objType, obj));
      } else if (obj instanceof Writable) {
        // Native tables sometimes introduce primitive Writable objects at this point
        recordMap.put(names.get(structIndex), fromWritable((Writable) obj));
      } else {
        // In case the deserializer is the DatasetSerDe
        recordMap.put(names.get(structIndex), serialize(obj, objType));
      }
    }

    // TODO Improve serialization logic - CDAP-11
    return new Text(GSON.toJson(recordMap));
  }

  @SuppressWarnings("unchecked")
  private Object serialize(Object obj, TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return obj;
      case LIST:
        return serializeList((List<Object>) obj, (ListTypeInfo) typeInfo);
      case MAP:
        return serializeMap((Map<Object, Object>) obj, (MapTypeInfo) typeInfo);
      case STRUCT:
        return serializeStruct((List<Object>) obj, (StructTypeInfo) typeInfo);
      case UNION:
        throw new UnsupportedOperationException("union not yet supported");
    }
    throw new IllegalArgumentException("Unknown category " + typeInfo.getCategory());
  }

  private Object serializeList(List<Object> list, ListTypeInfo typeInfo) {
    // need to recurse since it may contain structs
    TypeInfo elementType = typeInfo.getListElementTypeInfo();
    List<Object> serialized = Lists.newArrayListWithCapacity(list.size());
    for (int i = 0; i < list.size(); i++) {
      serialized.add(i, serialize(list.get(i), elementType));
    }
    return serialized;
  }

  private Object serializeMap(Map<Object, Object> map, MapTypeInfo typeInfo) {
    // need to recurse since it may contain structs
    Map<Object, Object> serialized = Maps.newHashMapWithExpectedSize(map.size());
    TypeInfo keyType = typeInfo.getMapKeyTypeInfo();
    TypeInfo valType = typeInfo.getMapValueTypeInfo();
    for (Map.Entry<Object, Object> mapEntry : map.entrySet()) {
      serialized.put(serialize(mapEntry.getKey(), keyType), serialize(mapEntry.getValue(), valType));
    }
    return serialized;
  }

  // a struct is represented as a list of objects
  private Object serializeStruct(List<Object> struct, StructTypeInfo typeInfo) {
    Map<String, Object> serialized = Maps.newHashMapWithExpectedSize(struct.size());
    List<TypeInfo> types = typeInfo.getAllStructFieldTypeInfos();
    List<String> names = typeInfo.getAllStructFieldNames();
    for (int i = 0; i < struct.size(); i++) {
      serialized.put(names.get(i), serialize(struct.get(i), types.get(i)));
    }
    return serialized;
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
