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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.format.UnexpectedFormatException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Converts an object with a schema into another type of object with the same schema.
 * For example, child implementations could convert a StructuredRecord to a GenericRecord and vice versa
 * as can be seen in AvroToStructuredTransformer and StructuredToAvroTransformer
 * @param <INPUT>
 * @param <OUTPUT>
 */
public abstract class RecordConverter<INPUT, OUTPUT> {

  abstract OUTPUT transform(INPUT record) throws IOException;

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

  private List<Object> convertArray(Object values, Schema elementSchema) throws IOException {
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

  private Map<String, Object> convertMap(Map<String, Object> map, Schema valueSchema) throws IOException {
    Map<String, Object> converted = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      converted.put(entry.getKey(), convertField(entry.getValue(), valueSchema));
    }
    return converted;
  }

  protected Object convertField(Object field, Schema fieldSchema) throws IOException {
    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case RECORD:
        return transform((INPUT) field);
      case ARRAY:
        return convertArray(field, fieldSchema.getElementType());
      case MAP:
        return convertMap((Map<String, Object>) field, fieldSchema.getValueType());
      case UNION:
        return convertUnion(field, fieldSchema.getTypes());
      case NULL:
        return null;
      case STRING:
        return field.toString();
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
}
