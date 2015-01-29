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

package co.cask.cdap.conversion.avro;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts stream events into avro records.
 */
public class Converter {
  private final Set<String> headerFields;
  private final Schema avroSchema;

  /**
   * Create a converter that can be used to convert stream events into avro records.
   *
   * @param avroSchema the schema for generated avro records
   * @param headerFields fields of the schema that should take their values from a stream event's headers
   */
  public Converter(Schema avroSchema, String[] headerFields) {
    Preconditions.checkArgument(avroSchema.getType() == Schema.Type.RECORD, "Only records are supported.");
    this.headerFields = Sets.newHashSet(headerFields);
    this.avroSchema = avroSchema;
  }

  /**
   * Convert a {@link StructuredRecord} of the stream event body, the stream event timestamp, and the headers
   * for the stream event into a {@link GenericRecord} of the given schema.
   *
   * @param streamBody the body of the stream event
   * @param timestamp the timestamp of the stream event
   * @param headers headers of the stream event
   * @return the stream event as an avro record
   * @throws UnexpectedFormatException if there was an error during conversion due to incompatible fields
   */
  public GenericRecord convert(Object streamBody, long timestamp, Map<String, String> headers) {
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      if ("ts".equals(fieldName)) {
        recordBuilder.set(fieldName, timestamp);
      } else if (headerFields.contains(fieldName)) {
        // if the field should come from the headers, take the value from the headers
        recordBuilder.set(fieldName, headers.get(fieldName));
      } else {
        // otherwise the field should come from the event body
        recordBuilder.set(fieldName, convertField(getRecordField(streamBody, fieldName), field.schema()));
      }
    }
    return recordBuilder.build();
  }

  private GenericRecord convertRecord(Object record, Schema avroSchema) {
    if (record instanceof GenericRecord) {
      return (GenericRecord) record;
    }
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      Schema fieldSchema = field.schema();
      Object fieldValue = getRecordField(record, fieldName);
      recordBuilder.set(fieldName, convertField(fieldValue, fieldSchema));
    }
    return recordBuilder.build();
  }

  private Object convertField(Object field, Schema fieldSchema) {
    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case RECORD:
        return convertRecord(field, fieldSchema);
      case ARRAY:
        return convertArray(field, fieldSchema.getElementType());
      case MAP:
        return convertMap((Map<String, Object>) field, fieldSchema.getValueType());
      case UNION:
        return convertUnion(field, fieldSchema.getTypes());
      case NULL:
        return null;
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return field;
      default:
        throw new UnexpectedFormatException("field type " + fieldType + " is not supported.");
    }
  }

  private List<Object> convertArray(Object values, Schema elementSchema) {
    List<Object> output;
    if (values instanceof List) {
      List<Object> valuesList = (List<Object>) values;
      output = Lists.newArrayListWithCapacity(valuesList.size());
      for (Object value : valuesList) {
        output.add(convertField(value, elementSchema));
      }
    } else {
      Object[] valuesArr = (Object[]) values;
      output = Lists.newArrayListWithCapacity(valuesArr.length);
      for (Object value : valuesArr) {
        output.add(convertField(value, elementSchema));
      }
    }
    return output;
  }

  private Map<String, Object> convertMap(Map<String, Object> map, Schema valueSchema) {
    Map<String, Object> converted = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      converted.put(entry.getKey(), convertField(entry.getValue(), valueSchema));
    }
    return converted;
  }

  private Object convertUnion(Object value, List<Schema> schemas) {
    boolean isNullable = false;
    for (Schema possibleSchema : schemas) {
      if (possibleSchema.getType() == Schema.Type.NULL) {
        isNullable = true;
        if (value == null) {
          return value;
        }
      } else {
        try {
          return convertField(value, possibleSchema);
        } catch (Exception e) {
          // if we couldn't convert, move to the next possibility
        }
      }
    }
    if (isNullable) {
      return null;
    }
    throw new UnexpectedFormatException("unable to determine union type.");
  }

  // get a field from the object using the get method if the object is a StructuredRecord,
  // or using reflection if it is not.
  private Object getRecordField(Object record, String fieldName) {
    try {
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
    } catch (Exception e) {
      throw new UnexpectedFormatException(e);
    }
  }
}
