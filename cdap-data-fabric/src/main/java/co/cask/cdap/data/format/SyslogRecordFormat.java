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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

/**
 * A Syslog protocol parser.
 *
 * It should be capable of parsing RFC 3164 (BSD syslog) streams as well as
 * RFC 5424 (defined in 2009.) They are widely different, so a few
 * if-statements look awful, most prominently the
 * is-this-a-version-number-or-a-year check.
 *
 * Since the tag and PID parts of RFC 3164 are highly optional and part of
 * the MSG, rather than the header, parsing them can be turned off.
 */
public class SyslogRecordFormat extends StreamEventRecordFormat<StructuredRecord> {
  private final SyslogParser parser = new SyslogParser();

  @Override
  public StructuredRecord read(StreamEvent event) throws UnexpectedFormatException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Map<String, String> fields = parser.parseMessage(Bytes.toString(event.getBody()), Charsets.UTF_8);
    Iterator it = fields.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> pair = (Map.Entry<String, String>) it.next();
      builder.convertAndSet(pair.getKey(), pair.getValue());
    }
    return builder.build();
  }

  @Override
  protected Schema getDefaultSchema() {
    return Schema.recordOf("streamEvent",
                           Schema.Field.of("facility", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                           Schema.Field.of("priority", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                           Schema.Field.of("severity", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                           Schema.Field.of("version", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                           Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                           Schema.Field.of("hostname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                           Schema.Field.of("msg", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  }

  @Override
  protected void validateSchema(Schema desiredSchema) throws UnsupportedTypeException {
    // a valid schema is a record of simple types.
    Iterator<Schema.Field> fields = desiredSchema.getFields().iterator();
    while (fields.hasNext()) {
      Schema.Field field = fields.next();
      Schema schema = field.getSchema();
      boolean isSimple = schema.getType().isSimpleType();
      boolean isNullableSimple = schema.isNullableSimple();
      if (!isSimple && !isNullableSimple) {
        throw new UnsupportedTypeException("Field " + field.getName() + " is of invalid type.");
      }
    }
  }
}
