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

package co.cask.cdap.api.spark.sql;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility class for conversions between {@link DataType} and {@link Schema}.
 */
public final class DataFrames {

  // A default union selector that will return the non-nullable part of it if the schema is nullable.
  // Otherwise exception will be thrown.
  private static final Function1<Schema, DataType> DEFAULT_UNION_SELECTOR = new AbstractFunction1<Schema, DataType>() {
    @Override
    public DataType apply(Schema schema) {
      if (schema.isNullable()) {
        return schemaToDataType(schema.getNonNullable(), this);
      }
      throw new IllegalArgumentException("Union schema is not support: " + schema);
    }
  };

  /**
   * Converts a {@link Schema} to Spark {@link DataType}.
   *
   * @param schema the schema to convert
   * @return The corresponding {@link DataType}
   */
  public static <T extends DataType> T toDataType(Schema schema) {
    return toDataType(schema, DEFAULT_UNION_SELECTOR);
  }

  /**
   * Converts a {@link Schema} to Spark {@link DataType}.
   *
   * @param schema the schema to convert
   * @param unionSelector the function to pick which schema to use when a {@link Schema.Type#UNION} type schema
   *                      is encountered
   * @return The corresponding {@link DataType}
   */
  public static <T extends DataType> T toDataType(Schema schema, Function1<Schema, DataType> unionSelector) {
    return (T) schemaToDataType(schema, unionSelector);
  }

  /**
   * Converts a Spark {@link DataType} to a {@link Schema} object.
   *
   * @param dataType the data type to convert from
   * @return The corresponding {@link Schema}
   */
  public static Schema toSchema(DataType dataType) {
    return dataTypeToSchema(dataType, new int[] { 0 });
  }

  /**
   * Creates a {@link Row} object that represents data in the given {@link StructuredRecord}.
   *
   * @param record contains the record data
   * @param structType a {@link StructType} representing the data type in the resulting {@link Row}.
   * @return a new {@link Row} instance
   */
  public static Row toRow(StructuredRecord record, StructType structType) {
    return (Row) toRowValue(record, structType, "");
  }

  /**
   * Creates a {@link StructuredRecord} from the data in the given {@link Row}.
   *
   * @param row contains the record data
   * @param schema the {@link Schema} of the resulting {@link StructuredRecord}.
   * @return a new {@link StructuredRecord} instance
   */
  public static StructuredRecord fromRow(Row row, Schema schema) {
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Only record type schema is supported");
    }
    return (StructuredRecord) fromRowValue(row, schema, "");
  }

  /**
   * Actual method to convert {@link Schema} to Spark {@link DataType}. It is separated out for the generic casting.
   *
   * @param schema the schema to convert
   * @param unionSelector the function to pick which schema to use when a {@link Schema.Type#UNION} type schema
   *                      is encountered
   * @return The corresponding {@link DataType}
   */
  private static DataType schemaToDataType(Schema schema, Function1<Schema, DataType> unionSelector) {
    switch (schema.getType()) {
      case NULL:
        return DataTypes.NullType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BYTES:
        return DataTypes.BinaryType;
      case STRING:
        return DataTypes.StringType;
      case ENUM:
        return DataTypes.StringType;
      case ARRAY:
        Schema componentSchema = schema.getComponentSchema();
        return DataTypes.createArrayType(schemaToDataType(componentSchema, unionSelector),
                                         componentSchema.isNullable());
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        return DataTypes.createMapType(schemaToDataType(mapSchema.getKey(), unionSelector),
                                       schemaToDataType(mapSchema.getValue(), unionSelector),
                                       mapSchema.getValue().isNullable());
      case RECORD:
        List<StructField> structFields = new ArrayList<>(schema.getFields().size());
        for (Schema.Field field : schema.getFields()) {
          Schema fieldSchema = field.getSchema();
          DataType fieldType = schemaToDataType(fieldSchema, unionSelector);
          structFields.add(DataTypes.createStructField(field.getName(), fieldType, fieldSchema.isNullable()));
        }
        return DataTypes.createStructType(structFields);
      case UNION:
        return unionSelector.apply(schema);
    }
    // Should not happen
    throw new IllegalArgumentException("Unsupported schema: " + schema);
  }


  /**
   * Converts a Spark {@link DataType} to a {@link Schema} object.
   *
   * @param dataType the data type to convert from
   * @param recordCounter tracks number of record schema becoming created; used for record name generation only
   * @return a new {@link Schema}.
   */
  private static Schema dataTypeToSchema(DataType dataType, int[] recordCounter) {
    if (dataType.equals(DataTypes.NullType)) {
      return Schema.of(Schema.Type.NULL);
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      return Schema.of(Schema.Type.BOOLEAN);
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.LongType)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return Schema.of(Schema.Type.FLOAT);
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return Schema.of(Schema.Type.DOUBLE);
    }
    if (dataType.equals(DataTypes.BinaryType)) {
      return Schema.of(Schema.Type.BYTES);
    }
    if (dataType.equals(DataTypes.StringType)) {
      return Schema.of(Schema.Type.STRING);
    }
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;

      // Special case for byte array
      if (arrayType.elementType() == DataTypes.ByteType) {
        return Schema.of(Schema.Type.BYTES);
      }

      Schema componentSchema = dataTypeToSchema(arrayType.elementType(), recordCounter);
      return Schema.arrayOf(arrayType.containsNull() ? Schema.nullableOf(componentSchema) : componentSchema);
    }
    if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      Schema valueSchema = dataTypeToSchema(mapType.valueType(), recordCounter);
      return Schema.mapOf(dataTypeToSchema(mapType.keyType(), recordCounter),
                          mapType.valueContainsNull() ? Schema.nullableOf(valueSchema) : valueSchema);
    }
    if (dataType instanceof StructType) {
      List<Schema.Field> fields = new ArrayList<>();
      for (StructField structField : ((StructType) dataType).fields()) {
        Schema fieldSchema = dataTypeToSchema(structField.dataType(), recordCounter);
        fields.add(Schema.Field.of(structField.name(),
                                   structField.nullable() ? Schema.nullableOf(fieldSchema) : fieldSchema));
      }
      return Schema.recordOf("Record" + recordCounter[0]++, fields);
    }

    // Some special types in Spark SQL
    if (dataType.equals(DataTypes.TimestampType)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return Schema.of(Schema.Type.LONG);
    }

    // Not support the CalendarInterval type for now, as there is no equivalent in Schema
    throw new IllegalArgumentException("Unsupported data type: " + dataType.typeName());
  }

  /**
   * Converts an object value to a value type acceptable by {@link Row}
   *
   * @param value the value to convert
   * @param dataType the target {@link DataType} of the value
   * @param path the current field path from the top. It is just for error message purpose.
   * @return an object that is compatible with Spark {@link Row}.
   */
  private static Object toRowValue(@Nullable Object value, DataType dataType, String path) {
    if (value == null) {
      return null;
    }
    if (dataType.equals(DataTypes.NullType)) {
      return null;
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      return value;
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return value;
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return value;
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return value;
    }
    if (dataType.equals(DataTypes.LongType)) {
      return value;
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return value;
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return value;
    }
    if (dataType.equals(DataTypes.BinaryType)) {
      if (value instanceof ByteBuffer) {
        return Bytes.toBytes((ByteBuffer) value);
      }
      return value;
    }
    if (dataType.equals(DataTypes.StringType)) {
      return value;
    }
    if (dataType instanceof ArrayType) {
      @SuppressWarnings("unchecked")
      Collection<Object> collection;
      int size;
      if (value instanceof Collection) {
        collection = (Collection<Object>) value;
      } else if (value.getClass().isArray()) {
        collection = Arrays.asList((Object[]) value);
      } else {
        throw new IllegalArgumentException(
          "Value type " + value.getClass() +
            " is not supported as array type value. It must either be a Collection or an array");
      }

      List<Object> result = new ArrayList<>(collection.size());
      String elementPath = path + "[]";
      ArrayType arrayType = (ArrayType) dataType;

      for (Object obj : collection) {
        Object elementValue = toRowValue(obj, arrayType.elementType(), elementPath);
        if (elementValue == null && !arrayType.containsNull()) {
          throw new IllegalArgumentException("Null value is not allowed for array element at " + elementPath);
        }
        result.add(elementValue);
      }
      return JavaConversions.asScalaBuffer(result).toSeq();
    }
    if (dataType instanceof MapType) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      Map<Object, Object> result = new LinkedHashMap<>(map.size());
      String mapPath = path + "<>";
      MapType mapType = (MapType) dataType;

      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object mapKey = toRowValue(entry.getKey(), mapType.keyType(), mapPath);
        if (mapKey == null) {
          throw new IllegalArgumentException("Null key is not allowed for map at " + mapPath);
        }
        Object mapValue = toRowValue(entry.getValue(), mapType.valueType(), mapPath);
        if (mapValue == null && !mapType.valueContainsNull()) {
          throw new IllegalArgumentException("Null value is not allowed for map at " + mapPath);
        }
        result.put(mapKey, mapValue);
      }
      return JavaConversions.mapAsScalaMap(result);
    }
    if (dataType instanceof StructType) {
      StructuredRecord record = (StructuredRecord) value;
      StructField[] fields = ((StructType) dataType).fields();
      Object[] fieldValues = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        String fieldName = fields[i].name();
        String fieldPath = path + "/" + fieldName;
        Object fieldValue = toRowValue(record.get(fieldName), fields[i].dataType(), fieldPath);

        if (fieldValue == null && !fields[i].nullable()) {
          throw new IllegalArgumentException("Null value is not allowed for row field at " + fieldPath);
        }
        fieldValues[i] = fieldValue;
      }
      return RowFactory.create(fieldValues);
    }

    // Some special types in Spark SQL
    if (dataType.equals(DataTypes.TimestampType)) {
      return new Timestamp((long) value);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return new Date((long) value);
    }

    // Not support the CalendarInterval type for now, as there is no equivalent in Schema
    throw new IllegalArgumentException("Unsupported data type: " + dataType.typeName());
  }

  /**
   * Converts a value from Spark {@link Row} into value acceptable for {@link StructuredRecord}.
   *
   * @param value the value to convert from
   * @param schema the target {@link Schema} of the value
   * @param path the current field path from the top. It is just for error message purpose.
   * @return a value object acceptable to be used in {@link StructuredRecord}.
   */
  private static Object fromRowValue(Object value, Schema schema, String path) {
    switch (schema.getType()) {
      // For all simple types, return as is.
      case NULL:
        return null;
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return value;
      case BYTES:
        return ByteBuffer.wrap((byte[]) value);
      case ARRAY: {
        // Value must be a collection
        @SuppressWarnings("unchecked")
        Collection<Object> collection = (Collection<Object>) value;
        List<Object> result = new ArrayList<>(collection.size());

        Schema componentSchema = schema.getComponentSchema();
        Schema valueSchema = getNonNullIfNullable(componentSchema);

        String elementPath = path + "[]";
        for (Object element : collection) {
          if (element == null && !componentSchema.isNullable()) {
            throw new IllegalArgumentException("Null value is not allowed for array element at " + elementPath);
          }
          result.add(fromRowValue(element, valueSchema, path));
        }
        return result;
      }
      case MAP: {
        // Value must be a Map
        Map<?, ?> map = (Map<?, ?>) value;
        Map<Object, Object> result = new LinkedHashMap<>(map.size());
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();

        // Map in Row object won't have null key, as StructType doesn't support it.
        Schema keySchema = getNonNullIfNullable(mapSchema.getKey());
        Schema valueSchema = getNonNullIfNullable(mapSchema.getValue());

        String mapPath = path + "<>";
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (entry.getValue() == null && !mapSchema.getValue().isNullable()) {
            throw new IllegalArgumentException("Null value is not allowed for map at " + mapPath);
          }
          result.put(fromRowValue(entry.getKey(), keySchema, path),
                     fromRowValue(entry.getValue(), valueSchema, path));
        }
        return result;
      }
      case RECORD: {
        // Value must be a Row
        Row row = (Row) value;
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        int idx = 0;
        for (Schema.Field field : schema.getFields()) {
          String fieldPath = path + "/" + field.getName();
          Schema fieldSchema = field.getSchema();

          if (row.isNullAt(idx) && !fieldSchema.isNullable()) {
            throw new NullPointerException("Null value is not allowed in record field at " + fieldPath);
          }

          fieldSchema = getNonNullIfNullable(fieldSchema);

          // If the value is null for the field, just continue without setting anything to the StructuredRecord
          if (row.isNullAt(idx)) {
            idx++;
            continue;
          }

          // Special case handling for ARRAY and MAP in order to get the Java type
          if (fieldSchema.getType() == Schema.Type.ARRAY) {
            builder.set(field.getName(), fromRowValue(row.getList(idx), fieldSchema, fieldPath));
          } else if (fieldSchema.getType() == Schema.Type.MAP) {
            builder.set(field.getName(), fromRowValue(row.getJavaMap(idx), fieldSchema, fieldPath));
          } else {
            Object fieldValue = row.get(idx);

            // Date and timestamp special return type handling
            if (fieldValue instanceof Date) {
              fieldValue = ((Date) fieldValue).getTime();
            } else if (fieldValue instanceof Timestamp) {
              fieldValue = ((Timestamp) fieldValue).getTime();
            }
            builder.set(field.getName(), fromRowValue(fieldValue, fieldSchema, fieldPath));
          }

          idx++;
        }
        return builder.build();
      }
    }

    throw new IllegalArgumentException("Unsupported schema: " + schema);
  }

  /**
   * Returns the non-nullable part of the given {@link Schema} if it is nullable; otherwise return it as is.
   */
  private static Schema getNonNullIfNullable(Schema schema) {
    return schema.isNullable() ? schema.getNonNullable() : schema;
  }

  private DataFrames() {
  }
}
