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
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

/**
 *
 */
public class DelimitedStringsRecordFormatTest {

  @Test
  public void testSimpleSchemaValidation() throws UnsupportedTypeException {
    Schema simpleSchema = Schema.recordOf("event",
                                          Schema.Field.of("f1", Schema.of(Schema.Type.BOOLEAN)),
                                          Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("f3", Schema.of(Schema.Type.FLOAT)),
                                          Schema.Field.of("f4", Schema.of(Schema.Type.DOUBLE)),
                                          Schema.Field.of("f5", Schema.of(Schema.Type.BYTES)),
                                          Schema.Field.of("f6", Schema.of(Schema.Type.STRING))
    );
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              simpleSchema, Collections.<String, String>emptyMap());
    format.initialize(formatSpec);
  }

  @Test
  public void testArrayOfNullableStringsSchema() throws UnsupportedTypeException {
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("arr", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))));
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              schema, Collections.<String, String>emptyMap());
    format.initialize(formatSpec);
  }

  @Test
  public void testNullableFieldsAllowedInSchema() throws UnsupportedTypeException {
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("f1", Schema.unionOf(Schema.of(Schema.Type.BOOLEAN), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f2", Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f3", Schema.unionOf(Schema.of(Schema.Type.FLOAT), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f4", Schema.unionOf(Schema.of(Schema.Type.DOUBLE), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f5", Schema.unionOf(Schema.of(Schema.Type.BYTES), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f6", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL)))
    );
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              schema, Collections.<String, String>emptyMap());
    format.initialize(formatSpec);
  }

  @Test
  public void testSimpleArraySchemaValidation() throws UnsupportedTypeException {
    Schema schema = Schema.recordOf("event",
                                    Schema.Field.of("f1", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("f2", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("f3", Schema.of(Schema.Type.FLOAT)),
                                    Schema.Field.of("f4", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("f5", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("f6", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("f7", Schema.arrayOf(Schema.of(Schema.Type.STRING)))
    );
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              schema, Collections.<String, String>emptyMap());
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    format.initialize(formatSpec);
  }

  @Test(expected = UnsupportedTypeException.class)
  public void testComplexArraySchemaValidation() throws UnsupportedTypeException {
    Schema mapSchema = Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING));
    Schema schema = Schema.recordOf("event", Schema.Field.of("f1", Schema.arrayOf(mapSchema)));
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              schema, Collections.<String, String>emptyMap());

    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    format.initialize(formatSpec);
  }


  @Test(expected = UnsupportedTypeException.class)
  public void testMapFieldInvalid() throws UnsupportedTypeException {
    Schema mapSchema = Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING));
    Schema schema = Schema.recordOf("event", Schema.Field.of("f1", mapSchema));
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              schema, Collections.<String, String>emptyMap());

    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    format.initialize(formatSpec);
  }

  @Test(expected = UnsupportedTypeException.class)
  public void testRecordFieldInvalid() throws UnsupportedTypeException {
    Schema recordSchema = Schema.recordOf("record", Schema.Field.of("recordField", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf("event", Schema.Field.of("f1", recordSchema));
    FormatSpecification formatSpec =
      new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                              schema, Collections.<String, String>emptyMap());

    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    format.initialize(formatSpec);
  }

  @Test
  public void testStringArrayFormat() throws UnsupportedTypeException, UnexpectedFormatException {
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    format.initialize(null);
    String body = "userX,actionY,itemZ";
    StructuredRecord output = format.read(ByteBuffer.wrap(Bytes.toBytes(body)));
    String[] actual = output.get("body");
    String[] expected = body.split(",");
    Assert.assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void testDelimiter() throws UnsupportedTypeException, UnexpectedFormatException {
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    FormatSpecification spec = new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                                                       null,
                                                       ImmutableMap.of(DelimitedStringsRecordFormat.DELIMITER, " "));
    format.initialize(spec);
    String body = "userX actionY itemZ";
    StructuredRecord output = format.read(ByteBuffer.wrap(Bytes.toBytes(body)));
    String[] actual = output.get("body");
    String[] expected = body.split(" ");
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testCSV() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.CSV, null, Collections.<String, String>emptyMap());
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String body = "userX,actionY,itemZ";
    StructuredRecord output = format.read(ByteBuffer.wrap(Bytes.toBytes(body)));
    String[] actual = output.get("body");
    String[] expected = body.split(",");
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testTSV() throws Exception {
    FormatSpecification spec = new FormatSpecification(Formats.TSV, null, Collections.<String, String>emptyMap());
    RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);

    String body = "userX\tactionY\titemZ";
    StructuredRecord output = format.read(ByteBuffer.wrap(Bytes.toBytes(body)));
    String[] actual = output.get("body");
    String[] expected = body.split("\t");
    Assert.assertArrayEquals(expected, actual);
  }

  @Test
  public void testFormatRecordWithSchema() throws UnsupportedTypeException, UnexpectedFormatException {
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("f1", Schema.unionOf(Schema.of(Schema.Type.BOOLEAN), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f2", Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f3", Schema.unionOf(Schema.of(Schema.Type.FLOAT), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f4", Schema.unionOf(Schema.of(Schema.Type.DOUBLE), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f5", Schema.unionOf(Schema.of(Schema.Type.BYTES), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f6", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("f7", Schema.arrayOf(Schema.of(Schema.Type.STRING)))
    );
    DelimitedStringsRecordFormat format = new DelimitedStringsRecordFormat();
    FormatSpecification spec = new FormatSpecification(DelimitedStringsRecordFormat.class.getCanonicalName(),
                                                       schema,
                                                       ImmutableMap.of(DelimitedStringsRecordFormat.DELIMITER, ","));
    format.initialize(spec);
    boolean booleanVal = false;
    int intVal = Integer.MAX_VALUE;
    float floatVal = Float.MAX_VALUE;
    double doubleVal = Double.MAX_VALUE;
    byte[] bytesVal = new byte[] { 0, 1, 2 };
    String stringVal = "foo bar";
    String[] arrayVal = new String[] { "extra1", "extra2", "extra3" };
    String body = new StringBuilder()
      .append(booleanVal).append(",")
      .append(intVal).append(",")
      .append(floatVal).append(",")
      .append(doubleVal).append(",")
      .append(Bytes.toStringBinary(bytesVal)).append(",")
      .append(stringVal).append(",")
      .append(arrayVal[0]).append(",")
      .append(arrayVal[1]).append(",")
      .append(arrayVal[2])
      .toString();

    StructuredRecord output = format.read(ByteBuffer.wrap(Bytes.toBytes(body)));
    Assert.assertEquals(booleanVal, output.get("f1"));
    Assert.assertEquals(intVal, output.get("f2"));
    Assert.assertEquals(floatVal, output.get("f3"));
    Assert.assertEquals(doubleVal, output.get("f4"));
    Assert.assertArrayEquals(bytesVal, (byte[]) output.get("f5"));
    Assert.assertEquals(stringVal, output.get("f6"));
    Assert.assertArrayEquals(arrayVal, (String[]) output.get("f7"));

    // now try with null fields.
    output = format.read(ByteBuffer.wrap(Bytes.toBytes("true,,3.14159,,,hello world,extra1")));
    Assert.assertTrue((Boolean) output.get("f1"));
    Assert.assertNull(output.get("f2"));
    Assert.assertTrue(Math.abs(3.14159 - (Float) output.get("f3")) < 0.000001);
    Assert.assertNull(output.get("f4"));
    Assert.assertNull(output.get("f5"));
    Assert.assertEquals("hello world", output.get("f6"));
    Assert.assertArrayEquals(new String[] {"extra1"}, (String[]) output.get("f7"));
  }
}
