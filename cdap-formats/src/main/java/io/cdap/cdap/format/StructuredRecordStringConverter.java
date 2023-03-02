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

package io.cdap.cdap.format;

import com.google.common.base.Splitter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.io.JsonDecoder;
import io.cdap.cdap.format.io.JsonEncoder;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumReader;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumWriter;
import io.cdap.cdap.format.utils.FormatUtils;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Utility class for converting {@link StructuredRecord} to and from json.
 */
public class StructuredRecordStringConverter {

  private static final JsonStructuredRecordDatumWriter JSON_DATUM_WRITER = new JsonStructuredRecordDatumWriter();
  private static final JsonStructuredRecordDatumReader JSON_DATUM_READER = new JsonStructuredRecordDatumReader(
      true);

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
    return record.getSchema().getFields().stream()
        .map(f -> fieldToString(record, f))
        .collect(Collectors.joining(delimiter));
  }

  /**
   * Converts a delimited string to a {@link StructuredRecord} based on the schema.
   */
  public static StructuredRecord fromDelimitedString(String delimitedString, String delimiter,
      Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Iterator<Schema.Field> fields = schema.getFields().iterator();

    for (String part : Splitter.on(delimiter).split(delimitedString)) {
      Schema.Field field = fields.next();
      parseAndSetFieldValue(builder, field, part);
    }

    return builder.build();
  }

  /**
   * Set field value in the supplied Structured Record Builder.
   *
   * @param builder Structured Record Builder instance
   * @param field field to set
   * @param part String portion of the delimited input
   */
  protected static void parseAndSetFieldValue(StructuredRecord.Builder builder,
      Schema.Field field,
      String part) {
    if (part.isEmpty()) {
      builder.set(field.getName(), null);
    } else {
      handleFieldConversion(builder, field, part);
    }
  }

  /**
   * Get the string representation for a given record field. BigDecimals are printed as plain
   * strings.
   *
   * @param record record to process
   * @param field field to extract
   * @return String representing the value for this field.
   */
  protected static String fieldToString(StructuredRecord record, Schema.Field field) {
    String fieldName = field.getName();
    Object value = record.get(fieldName);

    // Return null value as empty string.
    if (value == null) {
      return "";
    }

    // Get the field schema.
    Schema fieldSchema = field.getSchema();
    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    // Write decimal values as decimal strings.
    if (fieldSchema.getLogicalType() == Schema.LogicalType.DECIMAL) {
      BigDecimal decimalValue = record.getDecimal(fieldName);

      // Throw exception if the field is expected tu be decimal, but it could not be processed as such.
      if (decimalValue == null) {
        throw new IllegalArgumentException(
            "Invalid schema for field " + fieldName + ". Decimal was expected.");
      }
      return decimalValue.toPlainString();
    }

    // Output bytes fields as Base64
    if (fieldSchema.getType() == Schema.Type.BYTES) {
      try {
        return FormatUtils.base64Encode(value);
      } catch (IOException ioe) {
        throw new IllegalArgumentException("Invalid schema for field " + fieldName + ". "
            + "ByteBuffer or Byte Array was expected.", ioe);
      }
    }

    return value.toString();
  }

  /**
   * Handle field mapping and conversion into Structured Record
   *
   * @param field field schema to parse
   * @param part portion of the string to parse
   */
  protected static void handleFieldConversion(StructuredRecord.Builder builder,
      Schema.Field field,
      String part) {
    String fieldName = field.getName();

    // Get the underlying field schema
    Schema fieldSchema = field.getSchema();
    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    // read decimal values as decimal strings.
    if (fieldSchema.getLogicalType() == Schema.LogicalType.DECIMAL) {
      builder.setDecimal(fieldName, FormatUtils.parseDecimal(fieldSchema, part));
      return;
    }

    // Decode base64 payload for byte fields.
    if (fieldSchema.getType() == Schema.Type.BYTES) {
      try {
        builder.set(fieldName, FormatUtils.base64Decode(part));
        return;
      } catch (IOException ioe) {
        throw new IllegalArgumentException(
            "Unable to extract Base64 payload from field " + fieldName, ioe);
      }
    }

    // Handle all other field types
    builder.convertAndSet(fieldName, part);
  }

  protected StructuredRecordStringConverter() {
    //inaccessible constructor for static class
  }
}
