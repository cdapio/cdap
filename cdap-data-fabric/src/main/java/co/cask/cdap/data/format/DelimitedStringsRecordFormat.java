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

package co.cask.cdap.data.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.internal.format.StructuredRecord;
import co.cask.cdap.internal.format.UnexpectedFormatException;
import co.cask.cdap.internal.io.Schema;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

/**
 * Stream record format that interprets the body as string of delimited fields.
 */
public class DelimitedStringsRecordFormat extends ByteBufferRecordFormat<StructuredRecord> {
  public static final String CHARSET = "charset";
  public static final String DELIMITER = "delimiter";
  private Charset charset = Charsets.UTF_8;
  private String delimiter = ",";

  @Override
  public StructuredRecord read(ByteBuffer input) throws UnexpectedFormatException {
    String bodyAsStr = Bytes.toString(input, charset);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Iterator<String> bodyFields = Splitter.on(delimiter).split(bodyAsStr).iterator();
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      String fieldName = field.getName();
      if (fieldSchema.getType() == Schema.Type.ARRAY) {
        builder.set(fieldName, Lists.newArrayList(bodyFields).toArray(new String[0]));
      } else {
        String val = bodyFields.hasNext() ? bodyFields.next() : null;
        // if the body field is an empty string and the column is not a string type, interpret it as a null.
        if (val.isEmpty() && fieldSchema.getType() != Schema.Type.STRING) {
          val = null;
        }
        builder.convertAndSet(fieldName, val);
      }
    }
    return builder.build();
  }

  @Override
  protected Schema getDefaultSchema() {
    // default is a String[]
    return Schema.recordOf("streamEvent", Schema.Field.of("body", Schema.arrayOf(Schema.of(Schema.Type.STRING))));
  }

  @Override
  protected void validateDesiredSchema(Schema desiredSchema) throws UnsupportedTypeException {
    // a valid schema is a record of simple types. In other words, no maps, arrays, records, unions, or enums allowed.
    // the exception is the very last field, which is allowed to be an array of simple types.
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

  private boolean isStringArray(Schema schema) {
    return (schema.getType() == Schema.Type.ARRAY && schema.getComponentSchema().getType() == Schema.Type.STRING);
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
  }
}
