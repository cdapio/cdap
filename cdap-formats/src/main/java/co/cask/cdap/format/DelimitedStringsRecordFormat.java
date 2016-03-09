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

package co.cask.cdap.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.spi.stream.AbstractStreamEventRecordFormat;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Stream record format that interprets the body as string of delimited fields.
 *
 * <p>
 * The delimiter can be explicitly set through the "delimiter" setting, and the character set can also be set through
 * the "charset" setting. By default, the format will use a schema of one field, where the field is an array of strings.
 * The schema can be set to a schema of fields, with the i'th field corresponding to the i'th value in the delimited
 * text. Fields can also be parsed as scalar types - boolean, integer, long, double, float, bytes, and string.
 * In addition, the very last field can be an array of strings.
 * </p>
 *
 * <p>
 * If the "mapping" setting is provided, then we will use the mapping to parse the stream events rather than
 * the order of the schema fields. "mapping" is in the format "index0:field0,index1:field1,..".
 * For example, if "mapping" is "1:name,2:age", then a stream event like "sdf,bob,32,sdf,lkj" would be transformed into
 * a record {@code {"name":"bob", "age":32}}.
 * </p>
 */
public class DelimitedStringsRecordFormat extends AbstractStreamEventRecordFormat<StructuredRecord> {
  public static final String CHARSET = "charset";
  public static final String DELIMITER = "delimiter";
  public static final String MAPPING = "mapping";
  private Charset charset = Charsets.UTF_8;
  private String delimiter = ",";
  private RecordMaker recordMaker = new DefaultRecordMaker();

  @Override
  public StructuredRecord read(StreamEvent event) throws UnexpectedFormatException {
    String bodyAsStr = Bytes.toString(event.getBody(), charset);
    Iterator<String> bodyFields = Splitter.on(delimiter).split(bodyAsStr).iterator();
    return recordMaker.make(schema, bodyFields);
  }

  @Override
  protected Schema getDefaultSchema() {
    // default is a String[]
    return Schema.recordOf("streamEvent", Schema.Field.of("body", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
  }

  @Override
  protected void validateSchema(Schema desiredSchema) throws UnsupportedTypeException {
    // a valid schema is a record of simple types. In other words, no maps, arrays, records, unions, or enums allowed.
    // if mapping is null, the exception is the very last field, which is allowed to be an array of simple types.
    // These types may be nullable, which is a union of a null and non-null type.
    Iterator<Schema.Field> fields = desiredSchema.getFields().iterator();
    // check that each field is a simple field, except for the very last field, which can be an array of simple types.
    while (fields.hasNext()) {
      Schema.Field field = fields.next();
      Schema schema = field.getSchema();
      // if we're not on the very last field, the field must be a simple type or a nullable simple type.
      boolean isSimple = schema.getType().isSimpleType();
      boolean isNullableSimple = schema.isNullableSimple();
      if (!isSimple && !isNullableSimple) {
        // if this is the very last field and a string array, it is valid. otherwise it is not.
        if (fields.hasNext() || !isStringArray(schema)) {
          throw new UnsupportedTypeException("Field " + field.getName() + " is of invalid type.");
        }
      }
    }
  }

  @Override
  protected void configure(Map<String, String> settings) {
    String charsetStr = settings.get(CHARSET);
    if (charsetStr != null) {
      this.charset = Charset.forName(charsetStr);
    }
    String delimiter = settings.get(DELIMITER);
    if (delimiter != null) {
      this.delimiter = delimiter;
    }

    if (!getDefaultSchema().equals(schema)) {
      String mapping = settings.get(MAPPING);
      if (mapping != null) {
        this.recordMaker = new MappedSchemaRecordMaker(parseMapping(mapping, schema));
        for (Schema.Field field : schema.getFields()) {
          if (!field.getSchema().isSimpleOrNullableSimple()) {
            throw new IllegalArgumentException(
              String.format("only simple types allowed (field '%s') when the '%s' setting is present",
                            field.getName(), MAPPING));
          }
        }
      } else {
        this.recordMaker = new SchemaRecordMaker();
      }
    } else {
      this.recordMaker = new DefaultRecordMaker();
    }
  }

  // check that it's an array of strings or array of nullable strings. the array itself can also be nullable.
  private static boolean isStringArray(Schema schema) {
    Schema arrSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    if (arrSchema.getType() == Schema.Type.ARRAY) {
      Schema componentSchema = arrSchema.getComponentSchema();
      if (componentSchema.isNullable()) {
        return componentSchema.getNonNullable().getType() == Schema.Type.STRING;
      } else {
        return componentSchema.getType() == Schema.Type.STRING;
      }
    }
    return false;
  }

  private static String parseBodyValue(String val, Schema fieldSchema) {
    // if the body field is an empty string and the column is not a string type, interpret it as a null.
    if (val != null && val.isEmpty() && (fieldSchema.getType() != Schema.Type.STRING)) {
      return null;
    }
    return val;
  }

  private Map<String, Integer> parseMapping(String mappingString, Schema schema) {
    Splitter.MapSplitter splitter = Splitter.on(",").trimResults().withKeyValueSeparator(":");
    Map<String, String> stringMapping = splitter.split(mappingString);
    Preconditions.checkArgument(stringMapping.size() >= 1, "mapping cannot be empty");
    Preconditions.checkArgument(stringMapping.size() <= schema.getFields().size(),
                                "mapping cannot contain more entries than schema fields");

    Map<String, Integer> mapping = Maps.newHashMap();

    for (Map.Entry<String, String> entry : stringMapping.entrySet()) {
      String fieldIndexString = entry.getKey();
      String fieldName = entry.getValue();

      Preconditions.checkArgument(schema.getField(fieldName) != null,
                                  "schema is missing the mapped field " + fieldName);
      try {
        int fieldIndex = Integer.parseInt(fieldIndexString);
        mapping.put(fieldName, fieldIndex);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("mapping keys must be integer indices");
      }
    }

    return mapping;
  }

  /**
   * Makes a {@link StructuredRecord} in {@link DelimitedStringsRecordFormat#read(StreamEvent)}.
   */
  private interface RecordMaker {
    StructuredRecord make(Schema schema, Iterator<String> bodyFields);
  }

  /**
   * {@link RecordMaker} that uses the default schema.
   */
  private static class DefaultRecordMaker implements RecordMaker {

    @Override
    public StructuredRecord make(Schema schema, Iterator<String> bodyFields) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);

      List<String> fields = Lists.newArrayList(bodyFields);
      builder.set("body", fields.toArray(new String[fields.size()]));

      return builder.build();
    }
  }

  /**
   * {@link RecordMaker} that uses a schema.
   */
  private static class SchemaRecordMaker implements RecordMaker {

    @Override
    public StructuredRecord make(Schema schema, Iterator<String> bodyFields) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      Iterator<Schema.Field> fieldsIterator = schema.getFields().iterator();
      while (fieldsIterator.hasNext()) {
        Schema.Field field = fieldsIterator.next();
        Schema fieldSchema = field.getSchema();
        String fieldName = field.getName();
        if (isStringArray(fieldSchema)) {
          if (!fieldsIterator.hasNext()) {
            // only do varargs-style string array parsing on bodyField if it's the last field
            List<String> fields = Lists.newArrayList(bodyFields);
            builder.set(fieldName, fields.toArray(new String[fields.size()]));
          } else {
            throw new UnexpectedFormatException(
              String.format("string array type field '%s' must be the last schema field", fieldName));
          }
        } else {
          // simple type (not string array)
          String bodyField = bodyFields.hasNext() ? bodyFields.next() : null;
          String val = parseBodyValue(bodyField, fieldSchema);
          builder.convertAndSet(fieldName, val);
        }
      }
      return builder.build();
    }
  }

  /**
   * {@link RecordMaker} that uses the "mapping" setting and a schema.
   */
  private static class MappedSchemaRecordMaker implements RecordMaker {
    private final Map<String, Integer> mapping;
    private final int lastMappingIndex;

    private MappedSchemaRecordMaker(Map<String, Integer> mapping) {
      this.mapping = mapping;
      this.lastMappingIndex = Collections.max(mapping.values());
    }

    @Override
    public StructuredRecord make(Schema schema, Iterator<String> bodyFields) {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      // TODO: only read what's necessary from event.getBody() (e.g. if mapping is "0:f0", then only read first entry)
      List<String> fields = Lists.newArrayList(Iterators.limit(bodyFields, lastMappingIndex + 1));
      for (Schema.Field field : schema.getFields()) {
        Schema fieldSchema = field.getSchema();
        String fieldName = field.getName();
        int index = mapping.get(fieldName);
        if (index < fields.size()) {
          String val = parseBodyValue(fields.get(index), fieldSchema);
          builder.convertAndSet(fieldName, val);
        }
      }
      return builder.build();
    }
  }
}
