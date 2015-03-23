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
package co.cask.cdap.data.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

/**
 * PatternRecordFormat
 */
public class PatternRecordFormat extends StreamEventRecordFormat<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PatternRecordFormat.class);
  public static final String PATTERN = "pattern";
  private String pattern = "%{GREEDYDATA}";
  private Charset charset = Charsets.UTF_8;
  private final Grok grok = new Grok();

  public PatternRecordFormat() {
    try {
      grok.addPatternFromFile("/tmp/patterns");
      grok.addPatternFromFile("/tmp/java");
      grok.addPatternFromFile("/tmp/linux-syslog");
    } catch (GrokException e) {
      e.printStackTrace();
    }
  }

  @Override
  public StructuredRecord read(StreamEvent event) throws UnexpectedFormatException {
    String bodyAsStr = Bytes.toString(event.getBody(), charset);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Match gm = grok.match(bodyAsStr);
    gm.captures();
    Map<String, Object> x = gm.toMap();
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      builder.convertAndSet(fieldName, x.get(fieldName).toString());
    }

    return builder.build();
  }

  @Override
  protected Schema getDefaultSchema() {
    // default is a String[]
    return Schema.recordOf("streamEvent", Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  @Override
  protected void validateSchema(Schema desiredSchema) throws UnsupportedTypeException {
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

  // check that it's an array of strings or array of nullable strings. the array itself can also be nullable.
  private boolean isStringArray(Schema schema) {
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

  @Override
  protected void configure(Map<String, String> settings) {
    pattern = settings.get(PATTERN);
    try {
      grok.compile(pattern);
    } catch (GrokException e) {
      e.printStackTrace();
    }
  }
}
