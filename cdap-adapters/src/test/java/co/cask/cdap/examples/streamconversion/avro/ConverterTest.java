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

package co.cask.cdap.examples.streamconversion.avro;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class ConverterTest {

  @Test
  public void testSimpleConversion() throws Exception {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("event").fields()
      .requiredLong("ts")
      .optionalString("header1")
      .optionalString("header2")
      .requiredString("body")
      .endRecord();

    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

    Converter converter = new Converter(avroSchema, new String[] { "header1", "header2" });
    Map<String, String> headers = Maps.newHashMap();
    headers.put("header1", "value1");
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("body", "hello world")
      .set("headers", headers)
      .build();
    GenericRecord result = converter.convert(record, 1234567890, headers);

    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertEquals("value1", result.get("header1"));
    Assert.assertNull(result.get("header2"));
    Assert.assertEquals("hello world", result.get("body"));
  }

  @Test
  public void testScalarConversions() throws Exception {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("event").fields()
      .requiredLong("ts")
      .optionalInt("intField")
      .optionalBoolean("booleanField")
      .optionalBytes("bytesField")
      .optionalDouble("doubleField")
      .optionalFloat("floatField")
      .optionalString("stringField")
      .endRecord();

    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("intField", Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("booleanField", Schema.unionOf(Schema.of(Schema.Type.BOOLEAN), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("bytesField", Schema.unionOf(Schema.of(Schema.Type.BYTES), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("doubleField", Schema.unionOf(Schema.of(Schema.Type.DOUBLE), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("floatField", Schema.unionOf(Schema.of(Schema.Type.FLOAT), Schema.of(Schema.Type.NULL))),
      Schema.Field.of("stringField", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL))));

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("intField", Integer.MAX_VALUE)
      .set("booleanField", true)
      .set("bytesField", Bytes.toBytes("hello world"))
      .set("doubleField", Double.MAX_VALUE)
      .set("floatField", Float.MAX_VALUE)
      .set("stringField", "foo bar")
      .build();

    Converter converter = new Converter(avroSchema, new String[] { });
    GenericRecord result = converter.convert(record, 1234567890, Collections.<String, String>emptyMap());

    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertEquals(Integer.MAX_VALUE, result.get("intField"));
    Assert.assertEquals(true, result.get("booleanField"));
    Assert.assertArrayEquals(Bytes.toBytes("hello world"), (byte[]) result.get("bytesField"));
    Assert.assertEquals(Double.MAX_VALUE, result.get("doubleField"));
    Assert.assertEquals(Float.MAX_VALUE, result.get("floatField"));
    Assert.assertEquals("foo bar", result.get("stringField"));

    // check nulls
    result = converter.convert(StructuredRecord.builder(schema).build(),
                               1234567890, Collections.<String, String>emptyMap());
    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertNull(result.get("intField"));
    Assert.assertNull(result.get("booleanField"));
    Assert.assertNull(result.get("bytesField"));
    Assert.assertNull(result.get("doubleField"));
    Assert.assertNull(result.get("floatField"));
    Assert.assertNull(result.get("stringField"));
  }

  @Test
  public void testScalarMapConversion() {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("event").fields()
      .requiredLong("ts")
      .name("map")
        .type(org.apache.avro.Schema.createMap(
          org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))).noDefault()
      .endRecord();

    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));

    Map<String, String> mapValue = Maps.newHashMap();
    mapValue.put("key1", "val1");
    mapValue.put("key2", "val2");
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("map", mapValue)
      .build();

    Converter converter = new Converter(avroSchema, new String[] { });
    GenericRecord result = converter.convert(record, 1234567890, Collections.<String, String>emptyMap());

    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertEquals(mapValue, result.get("map"));
  }

  @Test
  public void testScalarArrayConversion() {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("event").fields()
      .requiredLong("ts")
      .name("arr")
      .type(org.apache.avro.Schema.createArray(
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))).noDefault()
      .endRecord();

    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("arr", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

    Converter converter = new Converter(avroSchema, new String[] { });

    // check using an array
    String[] value = new String[] { "foo", "bar", "baz" };
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("arr", value)
      .build();
    GenericRecord result = converter.convert(record, 1234567890, Collections.<String, String>emptyMap());
    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertEquals(Lists.newArrayList("foo", "bar", "baz"), result.get("arr"));

    // check using a list
    record = StructuredRecord.builder(schema)
      .set("arr", Lists.newArrayList("foo", "bar", "baz"))
      .build();
    result = converter.convert(record, 1234567890, Collections.<String, String>emptyMap());
    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertEquals(Lists.newArrayList("foo", "bar", "baz"), result.get("arr"));
  }

  @Test
  public void testNestedRecordConversion() {
    org.apache.avro.Schema innerRecordSchema = SchemaBuilder.record("inner").fields()
      .requiredInt("innerInt")
      .requiredString("innerString")
      .endRecord();
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("event").fields()
      .requiredLong("ts")
      .requiredInt("intField")
      .name("recordField").type(innerRecordSchema).noDefault()
      .endRecord();

    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("innerInt", Schema.of(Schema.Type.INT)),
      Schema.Field.of("innerString", Schema.of(Schema.Type.STRING)));
    Schema schema = Schema.recordOf(
      "event",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("recordField", innerSchema));

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("intField", 5)
      .set("recordField",
           StructuredRecord.builder(innerSchema)
             .set("innerInt", 7)
             .set("innerString", "hello world")
             .build()
      )
      .build();

    Converter converter = new Converter(avroSchema, new String[] { });
    GenericRecord result = converter.convert(record, 1234567890, Collections.<String, String>emptyMap());

    Assert.assertEquals(1234567890L, result.get("ts"));
    Assert.assertEquals(5, result.get("intField"));
    GenericRecord innerRecord = (GenericRecord) result.get("recordField");
    Assert.assertEquals(7, innerRecord.get("innerInt"));
    Assert.assertEquals("hello world", innerRecord.get("innerString"));
  }

  @Test
  public void testConvertGenericRecord() {

  }
}
