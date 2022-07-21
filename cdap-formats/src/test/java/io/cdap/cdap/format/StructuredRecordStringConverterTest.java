/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link StructuredRecordStringConverter} class from {@link StructuredRecord} to json string
 */
@SuppressWarnings("unchecked")
public class StructuredRecordStringConverterTest {
  Schema schema;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
  @Test
  public void testPrimitiveArrays() throws Exception {
    Schema arraysSchema = Schema.recordOf(
      "arrays",
      Schema.Field.of("int", Schema.arrayOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long", Schema.arrayOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float", Schema.arrayOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("bool", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))));
    StructuredRecord expected = StructuredRecord.builder(arraysSchema)
      .set("int", new int[]{Integer.MIN_VALUE, 0, Integer.MAX_VALUE})
      .set("long", new long[]{Long.MIN_VALUE, 0L, Long.MAX_VALUE})
      .set("float", new float[]{Float.MIN_VALUE, 0f, Float.MAX_VALUE})
      .set("double", new double[]{Double.MIN_VALUE, 0d, Double.MAX_VALUE})
      .set("bool", new boolean[]{false, true})
      .build();
    String recordOfJson = StructuredRecordStringConverter.toJsonString(expected);
    StructuredRecord actual = StructuredRecordStringConverter.fromJsonString(recordOfJson, arraysSchema);

    List<Integer> expectedInts = ImmutableList.of(Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
    List<Long> expectedLongs = ImmutableList.of(Long.MIN_VALUE, 0L, Long.MAX_VALUE);
    List<Float> expectedFloats = ImmutableList.of(Float.MIN_VALUE, 0f, Float.MAX_VALUE);
    List<Double> expectedDoubles = ImmutableList.of(Double.MIN_VALUE, 0d, Double.MAX_VALUE);
    List<Boolean> expectedBools = ImmutableList.of(false, true);

    Assert.assertEquals(expectedInts, actual.get("int"));
    Assert.assertEquals(expectedLongs, actual.get("long"));
    Assert.assertEquals(expectedFloats, actual.get("float"));
    Assert.assertEquals(expectedDoubles, actual.get("double"));
    Assert.assertEquals(expectedBools, actual.get("bool"));
  }

  @Test
  public void testNullableBytesJson() throws Exception {
    Schema s = Schema.recordOf("nullableBytes", Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    byte[] bytes = new byte[]{0, 1, 2};

    StructuredRecord byteBuf = StructuredRecord.builder(s).set("b", ByteBuffer.wrap(bytes)).build();
    StructuredRecord byteArr = StructuredRecord.builder(s).set("b", bytes).build();
    StructuredRecord nullRec = StructuredRecord.builder(s).build();

    String encoded = StructuredRecordStringConverter.toJsonString(byteBuf);
    StructuredRecord decoded = StructuredRecordStringConverter.fromJsonString(encoded, s);
    Assert.assertEquals("{\"b\":[0,1,2]}", encoded);
    Assert.assertArrayEquals(Bytes.toBytes((ByteBuffer) decoded.get("b")), bytes);

    encoded = StructuredRecordStringConverter.toJsonString(byteArr);
    decoded = StructuredRecordStringConverter.fromJsonString(encoded, s);
    Assert.assertEquals("{\"b\":[0,1,2]}", encoded);
    Assert.assertArrayEquals(Bytes.toBytes((ByteBuffer) decoded.get("b")), bytes);

    encoded = StructuredRecordStringConverter.toJsonString(nullRec);
    Assert.assertEquals("{\"b\":null}", encoded);
    decoded = StructuredRecordStringConverter.fromJsonString(encoded, s);
    Assert.assertNull(decoded.get("b"));
  }

  @Test
  public void testNullableBytesDelimited() throws Exception {
    Schema s = Schema.recordOf("nullableBytes", Schema.Field.of("b", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    byte[] bytes = new byte[]{0, 1, 2};

    StructuredRecord byteBuf = StructuredRecord.builder(s).set("b", ByteBuffer.wrap(bytes)).build();
    StructuredRecord byteArr = StructuredRecord.builder(s).set("b", bytes).build();
    StructuredRecord nullRec = StructuredRecord.builder(s).build();

    String encoded = StructuredRecordStringConverter.toDelimitedString(byteBuf, ",");
    StructuredRecord decoded = StructuredRecordStringConverter.fromDelimitedString(encoded, ",", s);
    Assert.assertEquals("AAEC", encoded);
    Assert.assertEquals(decoded.get("b"), ByteBuffer.wrap(bytes));

    encoded = StructuredRecordStringConverter.toDelimitedString(byteArr, ",");
    decoded = StructuredRecordStringConverter.fromDelimitedString(encoded, ",", s);
    Assert.assertEquals("AAEC", encoded);
    Assert.assertArrayEquals(((ByteBuffer) decoded.get("b")).array(), bytes);

    encoded = StructuredRecordStringConverter.toDelimitedString(nullRec, ",");
    Assert.assertEquals("", encoded);
    decoded = StructuredRecordStringConverter.fromDelimitedString(encoded, ",", s);
    Assert.assertNull(decoded.get("b"));
  }

  @Test
  public void checkConversion() throws Exception {
    for (boolean nullable : Arrays.asList(true, false)) {
      StructuredRecord initial = getStructuredRecord(nullable);
      String jsonOfRecord = StructuredRecordStringConverter.toJsonString(initial);
      StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, schema);
      assertRecordsEqual(initial, recordOfJson);
    }
  }

  @Test
  public void testDateLogicalTypeConversion() throws Exception {
    Schema dateSchema = Schema.recordOf(
      "date",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))));

    StructuredRecord record = StructuredRecord.builder(dateSchema).set("id", 1)
      .set("name", "alice")
      .setDate("date", LocalDate.now()).build();

    String jsonOfRecord = StructuredRecordStringConverter.toJsonString(record);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, dateSchema);

    assertRecordsEqual(record, recordOfJson);
  }

  @Test
  public void testTimeLogicalTypeConversion() throws Exception {
    Schema dateSchema = Schema.recordOf(
      "date",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("time", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))));

    StructuredRecord record = StructuredRecord.builder(dateSchema).set("id", 1)
      .set("name", "alice")
      .setTime("time", LocalTime.now()).build();

    String jsonOfRecord = StructuredRecordStringConverter.toJsonString(record);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, dateSchema);

    assertRecordsEqual(record, recordOfJson);
  }

  @Test
  public void testTimestampLogicalTypeConversion() throws Exception {
    Schema dateSchema = Schema.recordOf(
      "date",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))));

    StructuredRecord record = StructuredRecord.builder(dateSchema).set("id", 1)
      .set("name", "alice")
      .setTimestamp("timestamp", ZonedDateTime.now()).build();

    String jsonOfRecord = StructuredRecordStringConverter.toJsonString(record);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, dateSchema);

    assertRecordsEqual(record, recordOfJson);
  }

  @Test
  public void testDatetimeLogicalTypeConversion() throws Exception {
    Schema dateSchema = Schema.recordOf(
      "date",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("datetimefield", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));

    StructuredRecord record = StructuredRecord.builder(dateSchema).set("id", 1)
      .set("name", "alice")
      .setDateTime("datetimefield", LocalDateTime.now()).build();

    String jsonOfRecord = StructuredRecordStringConverter.toJsonString(record);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, dateSchema);

    assertRecordsEqual(record, recordOfJson);
  }

  //Ignore till CDAP-15317 is fixed
  @Ignore
  @Test
  public void testInvalidDatetimeLogicalTypeConversion() throws Exception {
    Schema dateSchema = Schema.recordOf(
      "date",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("datetime_field", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));

    StructuredRecord record = StructuredRecord.builder(dateSchema).set("id", 1)
      .set("name", "alice")
      .set("datetime_field", "random").build();
    thrown.expect(IOException.class);
    thrown.expectMessage("Unsupported format. DateTime type should be in ISO-8601 format.");
    StructuredRecordStringConverter.toJsonString(record);
  }

  //Ignore till CDAP-15317 is fixed
  @Ignore
  @Test
  public void testInvalidDatetimeLogicalTypeJSON() throws Exception {
    Schema dateSchema = Schema.recordOf(
      "date",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("datetime_field", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));
    String validDate = "2021-01-27T02:25:52.088";
    Map<String, String> jsonMap = new HashMap<>();
    jsonMap.put("id", "1");
    jsonMap.put("name", "alice");
    jsonMap.put("datetime_field", validDate);
    Gson gson = new Gson();
    String json = gson.toJson(jsonMap);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(json, dateSchema);
    Assert.assertEquals(LocalDateTime.parse(validDate), recordOfJson.getDateTime("datetime_field"));

    jsonMap.put("datetime_field", "1234");
    String invalidJson = gson.toJson(jsonMap);
    thrown.expect(IOException.class);
    thrown.expectMessage("Unsupported format. DateTime type should be in ISO-8601 format.");
    StructuredRecordStringConverter.fromJsonString(invalidJson, dateSchema);
  }

  @Test
  public void testDecimalLogicalTypeConversion() throws Exception {
    Schema dateSchema = Schema.recordOf("decimal",
                                        Schema.Field.of("d",
                                                        Schema.nullableOf(Schema.decimalOf(18, 7))));
    StructuredRecord record = StructuredRecord.builder(dateSchema)
      .setDecimal("d", new BigDecimal(new BigInteger("123456789123456789"), 7)).build();

    String jsonOfRecord = StructuredRecordStringConverter.toJsonString(record);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, dateSchema);

    Assert.assertEquals("{\"d\":12345678912.3456789}", jsonOfRecord);
    Assert.assertEquals(record.getDecimal("d"), recordOfJson.getDecimal("d"));

    String delimitedStringOfRecord = StructuredRecordStringConverter.toDelimitedString(record, ",");
    StructuredRecord recordOfDelimitedString =
      StructuredRecordStringConverter.fromDelimitedString(delimitedStringOfRecord, ",", dateSchema);

    Assert.assertEquals("12345678912.3456789", delimitedStringOfRecord);
    Assert.assertEquals(record.getDecimal("d"), recordOfDelimitedString.getDecimal("d"));
  }

  @Test
  public void testDelimitedWithNullsConversion() {
    Schema schema = Schema.recordOf("x",
                                    Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    StructuredRecord record = StructuredRecord.builder(schema).set("x", "abc").build();
    Assert.assertEquals("abc,", StructuredRecordStringConverter.toDelimitedString(record, ","));
    Assert.assertEquals(record, StructuredRecordStringConverter.fromDelimitedString("abc,", ",", schema));

    record = StructuredRecord.builder(schema).set("y", 5).build();
    Assert.assertEquals(",5", StructuredRecordStringConverter.toDelimitedString(record, ","));
    Assert.assertEquals(record, StructuredRecordStringConverter.fromDelimitedString(",5", ",", schema));
  }

  private StructuredRecord getStructuredRecord(boolean withNullValue) {
    Schema.Field mapField = Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                    Schema.of(Schema.Type.STRING)));
    Schema.Field idField = Schema.Field.of("id", Schema.of(Schema.Type.INT));
    Schema.Field nameField = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
    Schema.Field scoreField = Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE));
    Schema.Field graduatedField = Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN));
    Schema.Field binaryNameField = Schema.Field.of("binary", Schema.of(Schema.Type.BYTES));
    Schema.Field timeField = Schema.Field.of("time", Schema.of(Schema.Type.LONG));
    Schema.Field nullableField = Schema.Field.of("nullableField", Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    Schema.Field recordField = Schema.Field.of("innerRecord", Schema.recordOf("innerSchema", mapField, idField));
    StructuredRecord.Builder innerRecordBuilder = StructuredRecord.builder(Schema.recordOf("innerSchema",
                                                                                           mapField, idField));
    innerRecordBuilder
      .set("headers", ImmutableMap.of("a1", "a2"))
      .set("id", 3);

    schema = Schema.recordOf("complexRecord", mapField, idField, nameField, scoreField,
                             graduatedField, binaryNameField, timeField, nullableField, recordField);

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    recordBuilder
      .set("headers", ImmutableMap.of("h1", "v1"))
      .set("id", 1)
      .set("name", "Bob")
      .set("score", 3.4)
      .set("graduated", false)
      .set("time", System.currentTimeMillis())
      .set("binary", "Bob".getBytes(Charsets.UTF_8))
      .set("innerRecord", innerRecordBuilder.build());

    if (!withNullValue) {
      recordBuilder.set("nullableField", 123L);
    }

    return recordBuilder.build();
  }

  private void assertRecordsEqual(StructuredRecord one, StructuredRecord two) {
    Assert.assertTrue(one.getSchema().getRecordName().equals(two.getSchema().getRecordName()));
    Assert.assertTrue(one.getSchema().getFields().size() == two.getSchema().getFields().size());
    for (Schema.Field field : one.getSchema().getFields()) {
      Object oneFieldValue = one.get(field.getName());
      Object twoFieldValue = two.get(field.getName());

      if (oneFieldValue == null) {
        Assert.assertNull(twoFieldValue);
      } else if (oneFieldValue.getClass().equals(StructuredRecord.class)) {
        assertRecordsEqual((StructuredRecord) oneFieldValue, (StructuredRecord) twoFieldValue);
      } else if (field.getName().equals("binary")) {
        Assert.assertArrayEquals(asBytes(oneFieldValue), asBytes(twoFieldValue));
      } else {
        Assert.assertTrue(oneFieldValue.toString().equals(twoFieldValue.toString()));
      }
    }
  }

  /**
   * Returns a byte[] representing of the given obj. The object must either be a byte[] or {@link ByteBuffer}.
   */
  private byte[] asBytes(Object obj) {
    if (obj.getClass() == byte[].class) {
      return (byte[]) obj;
    }
    if (obj instanceof ByteBuffer) {
      return Bytes.getBytes((ByteBuffer) obj);
    }
    throw new IllegalArgumentException("Expected type to be byte[] or ByteBuffer. Got " + obj.getClass());
  }
}
