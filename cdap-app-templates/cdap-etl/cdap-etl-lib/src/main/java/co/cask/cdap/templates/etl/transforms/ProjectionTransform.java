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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformContext;
import co.cask.cdap.templates.etl.common.KeyValueListParser;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Projection transform that allows dropping, renaming, and converting field types.
 */
public class ProjectionTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final String DROP = "drop";
  private static final String RENAME = "rename";
  private static final String CONVERT = "convert";
  private static final Pattern fieldDelimiter = Pattern.compile("\\s*,\\s*");
  private Set<String> fieldsToDrop = Sets.newHashSet();
  private Map<String, String> fieldsToRename = Maps.newHashMap();
  private Map<String, Schema.Type> fieldsToConvert = Maps.newHashMap();
  // cache input schema hash to output schema so we don't have to build it each time
  private Map<Schema, Schema> schemaCache = Maps.newHashMap();

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(getClass().getSimpleName());
    configurer.setDescription("Projection transform that lets you drop, rename, and cast fields to a different type.");
    configurer.addProperty(new Property(
      DROP,
      "Comma separated list of fields to drop. For example: 'field1,field2,field3'.",
      false));
    configurer.addProperty(new Property(
      RENAME,
      "List of fields to rename. This is a comma separated list of key-value pairs, where each " +
        "pair is separated by a colon and specifies the input name and the output name. " +
        "For example: 'datestr:date,timestamp:ts' specifies that the 'datestr' field should be renamed to " +
        "'date' and the 'timestamp' field should be renamed to 'ts'.",
      false));
    configurer.addProperty(new Property(
      CONVERT,
      "List of fields to convert to a different type. This is a comma separated list of key-value pairs, where each " +
        "pair is separated by a colon and specifies the field name and the desired type. " +
        "For example: 'count:long,price:double' specifies that the 'count' field should be converted to a long " +
        "and the 'price' field should be converted to a double. Only simple types are supported " +
        "(boolean, int, long, float, double, bytes, string). Any simple type can be converted to bytes or a string. " +
        "Otherwise, a type can only be converted to a larger type. For example, an int can be converted to a long, " +
        "but a long cannot be converted to an int.",
      false));
  }

  @Override
  public void initialize(TransformContext context) {
    Map<String, String> properties = context.getRuntimeArguments();

    String dropStr = properties.get(DROP);
    if (dropStr != null) {
      for (String dropField : Splitter.on(fieldDelimiter).split(dropStr)) {
        fieldsToDrop.add(dropField);
      }
    }

    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    String renameStr = properties.get(RENAME);
    if (renameStr != null) {
      for (KeyValue<String, String> keyVal : kvParser.parse(renameStr)) {
        fieldsToRename.put(keyVal.getKey(), keyVal.getValue());
      }
    }

    String convertStr = properties.get(CONVERT);
    if (convertStr != null) {
      for (KeyValue<String, String> keyVal : kvParser.parse(convertStr)) {
        String name = keyVal.getKey();
        String typeStr = keyVal.getValue();
        Schema.Type type = Schema.Type.valueOf(typeStr.toUpperCase());
        if (!type.isSimpleType() || type == Schema.Type.NULL) {
          throw new IllegalArgumentException("Only non-null simple types are supported.");
        }
        fieldsToConvert.put(name, type);
      }
    }
  }

  @Override
  public void transform(StructuredRecord valueIn, Emitter<StructuredRecord> emitter) {
    Schema inputSchema = valueIn.getSchema();
    Schema outputSchema = getOutputSchema(inputSchema);
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field inputField : inputSchema.getFields()) {
      String inputFieldName = inputField.getName();
      if (fieldsToDrop.contains(inputFieldName)) {
        continue;
      }

      // get the corresponding output field name
      String outputFieldName = fieldsToRename.get(inputFieldName);
      if (outputFieldName == null) {
        outputFieldName = inputFieldName;
      }

      Schema.Field outputField = outputSchema.getField(outputFieldName);
      Object inputVal = valueIn.get(inputFieldName);

      // if we need to convert the value, convert it. otherwise just pass the value through
      if (fieldsToConvert.containsKey(inputFieldName)) {
        convertAndSet(builder, outputFieldName, inputVal, inputField.getSchema(), outputField.getSchema());
      } else {
        builder.set(outputFieldName, inputVal);
      }
    }
    emitter.emit(builder.build());
  }

  private void convertAndSet(StructuredRecord.Builder builder, String fieldName, Object val,
                             Schema inputSchema, Schema outputSchema) {
    // guaranteed that if the input type is nullable, the output type is also nullable.
    Schema.Type inputType = inputSchema.getType();
    Schema.Type outputType = outputSchema.getType();
    if (inputSchema.isNullable()) {
      if (val == null) {
        builder.set(fieldName, null);
      }
      inputType = inputSchema.getNonNullable().getType();
      outputType = outputSchema.getNonNullable().getType();
    }

    // if the input is a string, try and do some sensible conversion
    if (inputType == Schema.Type.STRING) {
      builder.convertAndSet(fieldName, (String) val);
    } else {
      // otherwise, just try to cast it.
      builder.set(fieldName, convertPrimitive(val, inputType, outputType));
    }
  }

  private Object convertPrimitive(Object val, Schema.Type inputType, Schema.Type outputType) {
    if (inputType == outputType) {
      return val;
    }

    // guaranteed input and output types are non-null simple types
    switch(inputType) {
      // if input is bytes, try to convert the bytes to the correct type
      case BYTES:
        byte[] bytesVal;
        if (val instanceof ByteBuffer) {
          bytesVal = Bytes.toBytes((ByteBuffer) val);
        } else {
          bytesVal = (byte[]) val;
        }
        switch(outputType) {
          case BOOLEAN:
            return Bytes.toBoolean(bytesVal);
          case INT:
            return Bytes.toInt(bytesVal);
          case LONG:
            return Bytes.toLong(bytesVal);
          case FLOAT:
            return Bytes.toFloat(bytesVal);
          case DOUBLE:
            return Bytes.toDouble(bytesVal);
          case STRING:
            return Bytes.toString(bytesVal);
        }
        break;
      case BOOLEAN:
        Boolean boolVal = (Boolean) val;
        switch (outputType) {
          case STRING:
            return String.valueOf(boolVal);
          case BYTES:
            return Bytes.toBytes(boolVal);
        }
        break;
      case INT:
        Integer intVal = (Integer) val;
        switch (outputType) {
          case LONG:
            return intVal.longValue();
          case FLOAT:
            return intVal.floatValue();
          case DOUBLE:
            return intVal.doubleValue();
          case STRING:
            return String.valueOf(intVal);
          case BYTES:
            return Bytes.toBytes(intVal);
        }
        break;
      case LONG:
        Long longVal = (Long) val;
        switch (outputType) {
          case FLOAT:
            return longVal.floatValue();
          case DOUBLE:
            return longVal.doubleValue();
          case STRING:
            return String.valueOf(longVal);
          case BYTES:
            return Bytes.toBytes(longVal);
        }
        break;
      case FLOAT:
        Float floatVal = (Float) val;
        switch (outputType) {
          case DOUBLE:
            return floatVal.doubleValue();
          case STRING:
            return String.valueOf(floatVal);
          case BYTES:
            return Bytes.toBytes(floatVal);
        }
        break;
      case DOUBLE:
        Double doubleVal = (Double) val;
        switch (outputType) {
          case STRING:
            return String.valueOf(doubleVal);
          case BYTES:
            return Bytes.toBytes(doubleVal);
        }
        break;
    }

    throw new IllegalArgumentException("Cannot convert type " + inputType + " to type " + outputType);
  }

  private Schema getOutputSchema(Schema inputSchema) {
    Schema output = schemaCache.get(inputSchema);
    if (output != null) {
      return output;
    }

    List<Schema.Field> outputFields = Lists.newArrayList();
    for (Schema.Field inputField : inputSchema.getFields()) {
      String inputFieldName = inputField.getName();
      if (fieldsToDrop.contains(inputFieldName)) {
        continue;
      }

      Schema outputFieldSchema = inputField.getSchema();
      // if this is a field that will be converted, figure out the desired schema
      if (fieldsToConvert.containsKey(inputFieldName)) {
        outputFieldSchema = Schema.of(fieldsToConvert.get(inputFieldName));
        Schema inputFieldSchema = inputField.getSchema();
        Schema.Type inputFieldType = inputFieldSchema.getType();

        // if the input was nullable, make the output nullable as well.
        if (inputFieldSchema.isNullable()) {
          inputFieldType = inputFieldSchema.getNonNullable().getType();
          outputFieldSchema = Schema.nullableOf(outputFieldSchema);
        }

        if (!inputFieldType.isSimpleType() || inputFieldType == Schema.Type.NULL) {
          throw new IllegalArgumentException("Field " + inputFieldName + " is of unconvertable type " + inputFieldType);
        }
      }

      String outputFieldName = inputFieldName;
      if (fieldsToRename.containsKey(inputFieldName)) {
        outputFieldName = fieldsToRename.get(inputFieldName);
      }

      outputFields.add(Schema.Field.of(outputFieldName, outputFieldSchema));
    }

    output = Schema.recordOf(inputSchema.getRecordName() + ".projected", outputFields);
    schemaCache.put(inputSchema, output);
    return output;
  }
}
