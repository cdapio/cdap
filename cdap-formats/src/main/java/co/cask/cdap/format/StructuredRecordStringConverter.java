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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.io.JsonDecoder;
import co.cask.cdap.format.io.JsonEncoder;
import co.cask.cdap.format.io.JsonStructuredRecordDatumReader;
import co.cask.cdap.format.io.JsonStructuredRecordDatumWriter;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;

/**
 * Utility class for converting {@link StructuredRecord} to and from json.
 */
public final class StructuredRecordStringConverter {

  private static final JsonStructuredRecordDatumWriter JSON_DATUM_WRITER = new JsonStructuredRecordDatumWriter();
  private static final JsonStructuredRecordDatumReader JSON_DATUM_READER = new JsonStructuredRecordDatumReader();

  /**
   * Converts a {@link StructuredRecord} to a json string.
   */
  public static String toJsonString(StructuredRecord record) throws IOException {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter writer = new JsonWriter(strWriter)) {
      JSON_DATUM_WRITER.encode(record, new JsonEncoder(writer));
      return strWriter.toString();
    }
  }

  /**
   * Converts a json string to a {@link StructuredRecord} based on the schema.
   */
  public static StructuredRecord fromJsonString(String json, Schema schema) throws IOException {
    try (JsonReader reader = new JsonReader(new StringReader(json))) {
      return JSON_DATUM_READER.read(new JsonDecoder(reader), schema);
    }
  }

  /**
   * Converts a {@link StructuredRecord} to a delimited string.
   */
  public static String toDelimitedString(final StructuredRecord record, String delimiter) {
    return Joiner.on(delimiter).join(
      Iterables.transform(record.getSchema().getFields(), new Function<Schema.Field, String>() {
        @Override
        public String apply(Schema.Field field) {
          return record.get(field.getName()).toString();
        }
      }));
  }

  /**
   * Converts a delimited string to a {@link StructuredRecord} based on the schema.
   */
  public static StructuredRecord fromDelimitedString(String delimitedString, String delimiter, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Iterator<Schema.Field> fields = schema.getFields().iterator();

    for (String part : Splitter.on(delimiter).split(delimitedString)) {
      if (!part.isEmpty()) {
        builder.convertAndSet(fields.next().getName(), part);
      }
    }

    return builder.build();
  }

  private StructuredRecordStringConverter() {
    //inaccessible constructor for static class
  }
}
