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
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

/**
 * Stream record format that interprets stream body as data in Combined Log Format.
 * CLF format: remote_host remote_login auth_user [date] "request" status content_length referrer user_agent
 * Sample CLF data:
 * 220.181.108.77 - - [01/Feb/2015:06:59:57 +0000] "GET / HTTP/1.1" 301 295 "-"
 * "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"
 *
 */
public class CombinedLogRecordFormat extends StreamEventRecordFormat<StructuredRecord> {

  @Override
  public StructuredRecord read(StreamEvent event) throws UnexpectedFormatException {
    String bodyAsStr = Bytes.toString(event.getBody());
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<String> parts = getLogEntries(bodyAsStr);
    List<Schema.Field> fields = schema.getFields();
    int index = 0;
    while (index < fields.size()) {
      Schema.Field field = fields.get(index);
      String val = (parts.size() < index || (parts.get(index).equals("-") &&
                                             field.getSchema().getType() != Schema.Type.STRING))
                    ? null : parts.get(index);
      builder.convertAndSet(fields.get(index).getName(), val);
      index++;
    }

    return builder.build();
  }


  @Override
  protected Schema getDefaultSchema() {
    return Schema.recordOf("streamEvent",
             Schema.Field.of("remote_host", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("remote_login ", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("auth_user", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("request", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("status", Schema.nullableOf(Schema.of(Schema.Type.INT))),
             Schema.Field.of("content_length", Schema.nullableOf(Schema.of(Schema.Type.INT))),
             Schema.Field.of("referrer", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
             Schema.Field.of("user_agent", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
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

  // parse CLF logEvent and get the record values.
  private List<String> getLogEntries(String logEvent) {
    List<String> parts = Lists.newArrayList();
    int start = 0;
    while (start < logEvent.length()) {
      if (logEvent.charAt(start) == ' ') {
        // Skip empty spaces
        start++;
      } else {
        start = addNextLogEntry(logEvent, start, parts);
      }
    }
    return parts;
  }

  // addNextLogEntry and return the start position of next entry.
  private int addNextLogEntry(String data, int start, List<String> parts) {
    int end = -1;
    if (data.charAt(start) == '"') {
      // Find the closing '"' and extract values within
      start = start + 1;
      end = findNext(data, start, '"');
    } else if (data.charAt(start) == '[') {
      // find the closing ']' and extract values
      start = start + 1;
      end = findNext(data, start, ']');
    } else {
      // find the next ' ' and extract values
      end = findNext(data, start + 1, ' ');
    }

    if (end == -1) {
      throw new UnexpectedFormatException(String.format("Could not parse data in CLF format. Entry %s", data));
    }

    parts.add(data.substring(start, end));
    return end + 1;
  }

  // Find the next character matching the "entry". Skip the entry that is escaped.
  private int findNext(String data, int startPosition, char entry) {
    int position = startPosition;
    int length = data.length();
    while (position < length) {
      if (data.charAt(position) == entry && (position == 0 || data.charAt(position - 1) != '\\')) {
        return position;
      }
      position++;
    }
    return -1;
  }
}
