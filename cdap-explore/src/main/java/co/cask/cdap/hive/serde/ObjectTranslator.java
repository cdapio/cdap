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

package co.cask.cdap.hive.serde;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Helper class for translating objects that fit a cdap {@link Schema} into objects
 * that Hive can understand.
 */
public class ObjectTranslator {

  /**
   * Using reflection, flatten an object into a list of fields so it can be examined by an ObjectInspector.
   * Assumes the field names and types given as input were derived from the schema of the object.
   *
   * @param obj object that fits a {@link Schema}.
   * @param fieldNames list of field names for the record.
   * @param fieldTypes list of field types for the record.
   * @return list of fields in the record, translated to be understandable by Hive.
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  public static List<Object> flattenRecord(Object obj, List<String> fieldNames, List<TypeInfo> fieldTypes)
    throws NoSuchFieldException, IllegalAccessException {
    List<Object> objectFields = Lists.newArrayListWithCapacity(fieldNames.size());
    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
      TypeInfo fieldType = fieldTypes.get(i);
      Object recordField = getRecordField(obj, fieldName);
      objectFields.add(translateField(recordField, fieldType));
    }
    return objectFields;
  }

  /**
   * Translate a field that fits a {@link Schema} field into a type that Hive understands.
   * For example, a ByteBuffer is allowed by schema but Hive only understands byte arrays, so all ByteBuffers must
   * be changed into byte arrays. Reflection is used to examine java objects if the expected hive type is a struct.
   *
   * @param field value of the field to translate.
   * @param typeInfo type of the field as expected by Hive.
   * @return translated field.
   * @throws NoSuchFieldException if a struct field was expected but not found in the object.
   * @throws IllegalAccessException if a struct field was not accessible.
   */
  public static Object translateField(Object field, TypeInfo typeInfo)
    throws NoSuchFieldException, IllegalAccessException {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return deserializePrimitive(field, (PrimitiveTypeInfo) typeInfo);
      case LIST:
        return deserializeList(field, (ListTypeInfo) typeInfo);
      case MAP:
        return deserializeMap(field, (MapTypeInfo) typeInfo);
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        ArrayList<String> innerFieldNames = structTypeInfo.getAllStructFieldNames();
        ArrayList<TypeInfo> innerFieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
        return flattenRecord(field, innerFieldNames, innerFieldTypes);
      case UNION:
        // TODO: decide what to do here
        return field;
    }
    return null;
  }

  /**
   * Translate a primitive type we understand into the type Hive understands. For example, we understand ByteBuffer
   * but Hive does not, so all ByteBuffer fields must be changed into byte[] fields.
   * See {@link co.cask.cdap.internal.io.AbstractSchemaGenerator} for the full mapping.
   * TODO: refactor so that changes don't have to be made both here and in AbstractSchemaGenerator
   */
  private static Object deserializePrimitive(Object primitive, PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
      case STRING:
        // URI, URL, and String all get mapped to string
        return primitive.toString();
      case BINARY:
        // byte[], ByteBuffer, and UUID get mapped to bytes
        if (primitive instanceof ByteBuffer) {
          return Bytes.toBytes((ByteBuffer) primitive);
        } else if (primitive instanceof UUID) {
          return Bytes.toBytes((UUID) primitive);
        } else {
          return primitive;
        }
      case INT:
        if (primitive instanceof Byte) {
          return ((Byte) primitive).intValue();
        } else if (primitive instanceof Character) {
          return (int) ((Character) primitive).charValue();
        } else if (primitive instanceof Short) {
          return ((Short) primitive).intValue();
        } else {
          return primitive;
        }
    }
    return primitive;
  }

  private static Object deserializeList(Object listField, ListTypeInfo typeInfo)
    throws NoSuchFieldException, IllegalAccessException {
    TypeInfo listElementType = typeInfo.getListElementTypeInfo();
    List<Object> hiveList = Lists.newArrayList();
    if (listField instanceof List) {
      for (Object obj : (List) listField) {
        hiveList.add(translateField(obj, listElementType));
      }
    } else {
      for (Object obj : (Object[]) listField) {
        hiveList.add(translateField(obj, listElementType));
      }
    }

    return hiveList;
  }

  private static Object deserializeMap(Object mapField, MapTypeInfo typeInfo)
    throws NoSuchFieldException, IllegalAccessException {
    Map<Object, Object> ourMap = (Map) mapField;
    TypeInfo keyType = typeInfo.getMapKeyTypeInfo();
    TypeInfo valType = typeInfo.getMapValueTypeInfo();
    Map translatedMap = Maps.newHashMap();
    for (Map.Entry entry : ourMap.entrySet()) {
      translatedMap.put(translateField(entry.getKey(), keyType),
                        translateField(entry.getValue(), valType));
    }
    return translatedMap;
  }

  // get a field from the object using the get method if the object is a StructuredRecord,
  // or using reflection if it is not.
  private static Object getRecordField(Object record, String fieldName)
    throws NoSuchFieldException, IllegalAccessException {
    if (record instanceof StructuredRecord) {
      return ((StructuredRecord) record).get(fieldName);
    }
    Class recordClass = record.getClass();
    Field field = recordClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(record);
  }
}
