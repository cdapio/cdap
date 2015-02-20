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
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Helper class for translating objects that fit a cdap {@link Schema} into objects
 * that Hive can understand.
 */
public class ObjectDeserializer {
  private final List<String> fieldNames;
  private final List<TypeInfo> fieldTypes;
  private final ObjectInspector inspector;
  // we can almost do without the schema. The problem is that everything in Hive is lowercase,
  // but when we look up record fields we need the case sensitive field name.
  private final Schema schema;

  /**
   * Creates an ObjectTranslator that will be able to deserialize objects that fit a {@link Schema} into objects
   * that a Hive ObjectInspector can understand.
   *
   * @param properties Properties object passed to a SerDe during initialization that contains the table columns
   */
  public ObjectDeserializer(Properties properties, Schema schema) {
    this(properties, schema, 0);
  }

  /**
   * Creates an ObjectTranslator that will be able to deserialize objects that fit a {@link Schema} into objects
   * that a Hive ObjectInspector can understand. Will ignore columns that are before the given field offset when
   * flattening records. The ObjectInspector will still use those columns.
   *
   * @param properties Properties object passed to a SerDe during initialization that contains the table columns
   * @param fieldOffset Ignore columns before the offset when flattening records
   */
  public ObjectDeserializer(Properties properties, Schema schema, int fieldOffset) {
    this(Lists.newArrayList(properties.getProperty(serdeConstants.LIST_COLUMNS).split(",")),
         TypeInfoUtils.getTypeInfosFromTypeString(properties.getProperty(serdeConstants.LIST_COLUMN_TYPES)),
         schema, fieldOffset);
  }

  public ObjectDeserializer(List<String> fieldNames, List<TypeInfo> fieldTypes, Schema schema) {
    this(fieldNames, fieldTypes, schema, 0);
  }

  @VisibleForTesting
  ObjectDeserializer(List<String> fieldNames, List<TypeInfo> fieldTypes, Schema schema, int fieldOffset) {
    this.fieldNames = fieldNames.subList(fieldOffset, fieldNames.size());
    this.fieldTypes = fieldTypes.subList(fieldOffset, fieldTypes.size());
    // inspector should still use all names and types passed in. This is in case there are some fields that are
    // determined outside of this class, such as the stream case where timestamp and headers are read elsewhere
    this.inspector = createInspector(fieldNames, fieldTypes);
    this.schema = schema;
  }

  /**
   * Get an ObjectInspector that Hive should use on the result of {@link #deserialize(Object)}.
   *
   * @return ObjectInspector that Hive should use on the result of {@link #deserialize(Object)}.
   */
  public ObjectInspector getInspector() {
    return inspector;
  }

  /**
   * Using reflection, deserialize an object that fits a {@link Schema} into one that can be examined
   * by an ObjectInspector.
   *
   * @param obj object that fits a {@link Schema}.
   * @return translated object that is understandable by Hive.
   * @throws NoSuchFieldException if a struct field was expected but not found in the object
   * @throws IllegalAccessException if a struct field was not accessible
   */
  public Object deserialize(Object obj) throws NoSuchFieldException, IllegalAccessException {
    if (fieldTypes.size() == 1) {
      return deserializeField(obj, fieldTypes.get(0), schema);
    } else {
      return flattenRecord(obj, fieldNames, fieldTypes, schema);
    }
  }

  /**
   * Using reflection, flatten an object into a list of fields so it can be examined by an ObjectInspector.
   * Assumes the field names and types given as input were derived from the schema of the object.
   *
   * @param obj object that fits a {@link Schema}.
   * @return list of fields in the record, translated to be understandable by Hive.
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  public List<Object> translateRecord(Object obj) throws NoSuchFieldException, IllegalAccessException {
    return flattenRecord(obj, fieldNames, fieldTypes, schema);
  }

  private List<Object> flattenRecord(Object obj, List<String> fieldNames, List<TypeInfo> fieldTypes,
                                     Schema schema) throws NoSuchFieldException, IllegalAccessException {
    boolean isNullable = schema.isNullable();
    if (obj == null) {
      if (isNullable) {
        return null;
      } else {
        throw new UnexpectedFormatException("Non-nullable field is null.");
      }
    }
    if (isNullable) {
      schema = schema.getNonNullable();
    }

    Map<String, Schema.Field> fieldMap = getFieldMap(schema);
    List<Object> objectFields = Lists.newArrayListWithCapacity(fieldNames.size());
    for (int i = 0; i < fieldNames.size(); i++) {
      String hiveName = fieldNames.get(i);
      TypeInfo fieldType = fieldTypes.get(i);
      Schema.Field schemaField = fieldMap.get(hiveName);
      // use the name from the schema field in case it is not all lowercase
      Object recordField = getRecordField(obj, schemaField.getName());
      objectFields.add(deserializeField(recordField, fieldType, schemaField.getSchema()));
    }
    return objectFields;
  }

  /**
   * Translate a field that fits a {@link Schema} field into a type that Hive understands.
   * For example, a ByteBuffer is allowed by schema but Hive only understands byte arrays, so all ByteBuffers must
   * be changed into byte arrays. Reflection is used to examine java objects if the expected hive type is a struct.
   *
   * @param field value of the field to deserialize.
   * @param typeInfo type of the field as expected by Hive.
   * @param schema schema of the field.
   * @return translated field.
   * @throws NoSuchFieldException if a struct field was expected but not found in the object.
   * @throws IllegalAccessException if a struct field was not accessible.
   */
  private Object deserializeField(Object field, TypeInfo typeInfo,
                                  Schema schema) throws NoSuchFieldException, IllegalAccessException {
    boolean isNullable = schema.isNullable();
    if (field == null) {
      if (isNullable) {
        return null;
      } else {
        throw new UnexpectedFormatException("Non-nullable field was null.");
      }
    }
    if (isNullable) {
      schema = schema.getNonNullable();
    }

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return deserializePrimitive(field, (PrimitiveTypeInfo) typeInfo);
      case LIST:
        // HIVE!! some versions will turn bytes into array<tinyint> instead of binary... so special case it.
        // TODO: remove once CDAP-1556 is done
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        if (isByteArray(listTypeInfo) && !(field instanceof Collection)) {
          return deserializeByteArray(field);
        }
        return deserializeList(field, (ListTypeInfo) typeInfo, schema.getComponentSchema());
      case MAP:
        return deserializeMap(field, (MapTypeInfo) typeInfo, schema.getMapSchema());
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        ArrayList<String> innerFieldNames = structTypeInfo.getAllStructFieldNames();
        ArrayList<TypeInfo> innerFieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
        return flattenRecord(field, innerFieldNames, innerFieldTypes, schema);
      case UNION:
        // TODO: decide what to do here
        return field;
    }
    return null;
  }

  private boolean isByteArray(ListTypeInfo typeInfo) {
    TypeInfo elementType = typeInfo.getListElementTypeInfo();
    return (elementType.getCategory().equals(ObjectInspector.Category.PRIMITIVE) &&
      ((PrimitiveTypeInfo) elementType).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.BYTE));
  }

  // Hive's object inspector will try to cast to Object[] so we can't return a byte[]...
  // TODO: remove once once CDAP-1556 is done
  private Byte[] deserializeByteArray(Object primitive) {
    // byte[], ByteBuffer, and UUID get mapped to bytes
    byte[] raw;
    if (primitive instanceof ByteBuffer) {
      ByteBuffer bb = (ByteBuffer) primitive;
      int length = bb.remaining();
      Byte[] output = new Byte[length];
      int pos = bb.position();
      for (int i = 0; i < length; i++) {
        output[i] = bb.get();
      }
      bb.position(pos);
      return output;
    } else if (primitive instanceof UUID) {
      raw = Bytes.toBytes((UUID) primitive);
    } else {
      raw = (byte[]) primitive;
    }
    Byte[] output = new Byte[raw.length];
    for (int i = 0; i < output.length; i++) {
      output[i] = raw[i];
    }
    return output;
  }

  /**
   * Translate a primitive type we understand into the type Hive understands. For example, we understand ByteBuffer
   * but Hive does not, so all ByteBuffer fields must be changed into byte[] fields.
   * See {@link co.cask.cdap.internal.io.AbstractSchemaGenerator} for the full mapping.
   * TODO: refactor so that changes don't have to be made both here and in AbstractSchemaGenerator
   */
  private Object deserializePrimitive(Object primitive, PrimitiveTypeInfo typeInfo) {
    switch (typeInfo.getPrimitiveCategory()) {
      case STRING:
        // URI, URL, and String all get mapped to string
        // Avro's utf8 also requires .toString()
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
          return (int) (Character) primitive;
        } else if (primitive instanceof Short) {
          return ((Short) primitive).intValue();
        } else {
          return primitive;
        }
    }
    return primitive;
  }

  private Object deserializeList(Object listField, ListTypeInfo typeInfo,
                                 Schema elementSchema) throws NoSuchFieldException, IllegalAccessException {
    TypeInfo listElementType = typeInfo.getListElementTypeInfo();
    List<Object> hiveList = Lists.newArrayList();
    if (listField instanceof Collection) {
      for (Object obj : (Collection<?>) listField) {
        hiveList.add(deserializeField(obj, listElementType, elementSchema));
      }
    } else {
      for (int i = 0; i < Array.getLength(listField); i++) {
        hiveList.add(deserializeField(Array.get(listField, i), listElementType, elementSchema));
      }
    }

    return hiveList;
  }

  @SuppressWarnings("unchecked")
  private Object deserializeMap(Object mapField, MapTypeInfo typeInfo, Map.Entry<Schema, Schema> mapSchema)
    throws NoSuchFieldException, IllegalAccessException {

    Map<Object, Object> ourMap = (Map) mapField;
    TypeInfo keyType = typeInfo.getMapKeyTypeInfo();
    TypeInfo valType = typeInfo.getMapValueTypeInfo();
    Schema keySchema = mapSchema.getKey();
    Schema valSchema = mapSchema.getValue();
    Map translatedMap = Maps.newHashMap();
    for (Map.Entry entry : ourMap.entrySet()) {
      translatedMap.put(deserializeField(entry.getKey(), keyType, keySchema),
                        deserializeField(entry.getValue(), valType, valSchema));
    }
    return translatedMap;
  }

  // get a field from the object using the get method if the object is a StructuredRecord,
  // or using reflection if it is not.
  private Object getRecordField(Object record, String fieldName)
    throws NoSuchFieldException, IllegalAccessException {
    if (record instanceof StructuredRecord) {
      return ((StructuredRecord) record).get(fieldName);
    }
    if (record instanceof GenericRecord) {
      return ((GenericRecord) record).get(fieldName);
    }
    Class recordClass = record.getClass();
    Field field = recordClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(record);
  }

  // get a map from the expected hive name of a field in the schema to the field in the schema.
  private Map<String, Schema.Field> getFieldMap(Schema schema) {
    Map<String, Schema.Field> fieldMap = Maps.newHashMap();
    for (Schema.Field field : schema.getFields()) {
      fieldMap.put(field.getName().toLowerCase(), field);
    }
    return fieldMap;
  }

  private ObjectInspector createInspector(List<String> fieldNames, List<TypeInfo> fieldTypes) {
    List<ObjectInspector> fieldInspectors = Lists.newArrayListWithCapacity(fieldTypes.size());
    for (TypeInfo typeInfo : fieldTypes) {
      fieldInspectors.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
  }
}
