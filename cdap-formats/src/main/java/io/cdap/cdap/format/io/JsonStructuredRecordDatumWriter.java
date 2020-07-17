/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.format.io;

import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.io.Encoder;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StructuredRecordDatumWriter} for encoding {@link StructuredRecord} to json.
 */
public class JsonStructuredRecordDatumWriter extends StructuredRecordDatumWriter {
  private final boolean logicalTypeAsString;

  public JsonStructuredRecordDatumWriter() {
    this.logicalTypeAsString = false;
  }

  public JsonStructuredRecordDatumWriter(boolean logicalTypeAsString) {
    this.logicalTypeAsString = logicalTypeAsString;
  }

  @Override
  public void encode(StructuredRecord data, Encoder encoder) throws IOException {
    if (!(encoder instanceof JsonEncoder)) {
      throw new IOException("The JsonStructuredRecordDatumWriter can only encode using a JsonEncoder");
    }
    super.encode(data, encoder);
  }

  @Override
  protected void encodeEnum(Encoder encoder, Schema enumSchema, Object value) throws IOException {
    String enumValue = value instanceof Enum ? ((Enum) value).name() : value.toString();
    encoder.writeString(enumValue);
  }

  @Override
  protected void encodeArrayBegin(Encoder encoder, Schema elementSchema, int size) throws IOException {
    getJsonWriter(encoder).beginArray();
  }

  @Override
  protected void encodeArrayEnd(Encoder encoder, Schema elementSchema, int size) throws IOException {
    getJsonWriter(encoder).endArray();
  }

  @Override
  protected void encodeMapBegin(Encoder encoder, Schema keySchema, Schema valueSchema, int size) throws IOException {
    if (!keySchema.isCompatible(Schema.of(Schema.Type.STRING))) {
      throw new IOException("Complex key type not supported: " + keySchema);
    }
    getJsonWriter(encoder).beginObject();
  }

  @Override
  protected void encodeMapEntry(Encoder encoder, Schema keySchema,
                                Schema valueSchema, Map.Entry<?, ?> entry) throws IOException {
    getJsonWriter(encoder).name(entry.getKey().toString());
    encode(encoder, valueSchema, entry.getValue());
  }

  @Override
  protected void encodeMapEnd(Encoder encoder, Schema keySchema, Schema valueSchema, int size) throws IOException {
    getJsonWriter(encoder).endObject();
  }

  @Override
  protected void encodeRecordBegin(Encoder encoder, Schema schema) throws IOException {
    getJsonWriter(encoder).beginObject();
  }

  @Override
  protected void encodeRecordField(Encoder encoder, Schema.Field field, Object value) throws IOException {
    getJsonWriter(encoder).name(field.getName());
    try {
      encode(encoder, field.getSchema(), value);
    } catch (ClassCastException e) {
      // happens if the record is constructed incorrectly.
      throw new IllegalArgumentException(
        String.format("A value for field '%s' is of type '%s', which does not match schema '%s'. ",
                      field.getName(), value.getClass().getName(), field.getSchema()));
    }
  }

  @Override
  protected void encodeRecordEnd(Encoder encoder, Schema schema) throws IOException {
    getJsonWriter(encoder).endObject();
  }

  @Override
  protected void encodeUnion(Encoder encoder, Schema schema, int matchingIdx, Object value) throws IOException {
    encode(encoder, schema.getUnionSchema(matchingIdx), value);
  }

  private JsonWriter getJsonWriter(Encoder encoder) {
    // Type already checked in the encode method, hence assuming the casting is fine.
    return ((JsonEncoder) encoder).getJsonWriter();
  }

  @Override
  protected void encode(Encoder encoder, Schema schema, Object value) throws IOException {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();

    if (logicalTypeAsString && logicalType != null) {
      switch (logicalType) {
        case DATE:
          Integer date = (Integer) value;
          // will be encoded to string of format YYYY-mm-DD
          encoder.writeString(LocalDate.ofEpochDay(date.longValue()).format(DateTimeFormatter.ISO_LOCAL_DATE));
          break;
        case TIME_MILLIS:
          LocalTime localTimeMillis = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) value)));
          // will be encoded to string of format HH:mm:ss.SSSSSSSSS
          encoder.writeString(localTimeMillis.format(DateTimeFormatter.ISO_LOCAL_TIME));
          break;
        case TIME_MICROS:
          LocalTime localTimeMicros = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
          // will be encoded to string of format HH:mm:ss.SSSSSSSSS
          encoder.writeString(localTimeMicros.format(DateTimeFormatter.ISO_LOCAL_TIME));
          break;
        case TIMESTAMP_MILLIS:
          ZonedDateTime timestampMillis = getZonedDateTime((long) value, TimeUnit.MILLISECONDS,
                                                           ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          // will be encoded to string of format YYYY-mm-DDTHH:mm:ss.SSSSSSSSSZ[UTC]
          encoder.writeString(timestampMillis.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
          break;
        case TIMESTAMP_MICROS:
          ZonedDateTime timestampMicros = getZonedDateTime((long) value, TimeUnit.MICROSECONDS,
                                                           ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          // will be encoded to string of format YYYY-mm-DDTHH:mm:ss.SSSSSSSSSZ[UTC]
          encoder.writeString(timestampMicros.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
          break;
        case DECIMAL:
          int scale = nonNullableSchema.getScale();
          BigDecimal bigDecimal;
          if (value instanceof ByteBuffer) {
            bigDecimal = new BigDecimal(new BigInteger(Bytes.toBytes((ByteBuffer) value)), scale);
          } else {
            bigDecimal = new BigDecimal(new BigInteger((byte[]) value), scale);
          }
          encoder.writeString(bigDecimal.toString());
          break;
      }
      return;
    }
    super.encode(encoder, schema, value);
  }

  /**
   * Get zoned date and time represented by the field.
   *
   * @param ts timestamp to be used to get {@link ZonedDateTime}
   * @param zoneId zone id for the field
   * @param unit time unit for ts
   * @return {@link ZonedDateTime} represented by field.
   */
  private ZonedDateTime getZonedDateTime(long ts, TimeUnit unit, ZoneId zoneId) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }
}
