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
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Test for {@link StructuredRecordStringConverter} class from {@link StructuredRecord} to json string
 */
@SuppressWarnings("unchecked")
public class StructuredRecordStringConverterTest {
  Schema schema;

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
      .set("int", new int[]{ Integer.MIN_VALUE, 0, Integer.MAX_VALUE })
      .set("long", new long[] { Long.MIN_VALUE, 0L, Long.MAX_VALUE })
      .set("float", new float[] { Float.MIN_VALUE, 0f, Float.MAX_VALUE })
      .set("double", new double[] { Double.MIN_VALUE, 0d, Double.MAX_VALUE })
      .set("bool", new boolean[] { false, true })
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
  public void checkConversion() throws Exception {
    for (boolean nullable : Arrays.asList(true, false)) {
      StructuredRecord initial = getStructuredRecord(nullable);
      String jsonOfRecord = StructuredRecordStringConverter.toJsonString(initial);
      StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, schema);
      assertRecordsEqual(initial, recordOfJson);
    }
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
