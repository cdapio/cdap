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
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AvroRecordFormatTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  @Test
  public void testFlatRecord() throws Exception {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))),
      Schema.Field.of("nullable", Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL)))
    );
    FormatSpecification formatSpecification = new FormatSpecification(
      RecordFormats.AVRO,
      schema,
      Collections.<String, String>emptyMap()
    );

    org.apache.avro.Schema avroSchema = convertSchema(schema);
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("long", Long.MAX_VALUE)
      .set("boolean", false)
      .set("bytes", ByteBuffer.wrap(Bytes.toBytes("hello world")))
      .set("double", Double.MAX_VALUE)
      .set("float", Float.MAX_VALUE)
      .set("string", "foo bar")
      .set("array", Lists.newArrayList(1, 2, 3))
      .set("map", ImmutableMap.of("k1", 1, "k2", 2))
      .set("nullable", null)
      .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);

    writer.write(record, encoder);
    encoder.flush();
    out.close();
    byte[] serializedRecord = out.toByteArray();

    AvroRecordFormat format = new AvroRecordFormat();
    format.initialize(formatSpecification);

    GenericRecord actual = format.read(ByteBuffer.wrap(serializedRecord));
    Assert.assertEquals(Integer.MAX_VALUE, actual.get("int"));
    Assert.assertEquals(Long.MAX_VALUE, actual.get("long"));
    Assert.assertFalse((Boolean) actual.get("boolean"));
    Assert.assertArrayEquals(Bytes.toBytes("hello world"), Bytes.toBytes((ByteBuffer) actual.get("bytes")));
    Assert.assertEquals(Double.MAX_VALUE, actual.get("double"));
    Assert.assertEquals(Float.MAX_VALUE, actual.get("float"));
    Assert.assertEquals("foo bar", actual.get("string").toString());
    Assert.assertEquals(Lists.newArrayList(1, 2, 3), actual.get("array"));
    assertMapEquals(ImmutableMap.<String, Object>of("k1", 1, "k2", 2), (Map<Object, Object>) actual.get("map"));
    Assert.assertNull(actual.get("nullable"));
  }

  @Test
  public void testNestedRecord() throws Exception {
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)));
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("record", innerSchema));
    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    org.apache.avro.Schema avroSchema = convertSchema(schema);

    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("record", new GenericRecordBuilder(avroInnerSchema).set("int", 5).set("double", 3.14159).build())
      .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);

    writer.write(record, encoder);
    encoder.flush();
    out.close();
    byte[] serializedRecord = out.toByteArray();

    FormatSpecification formatSpecification = new FormatSpecification(
      RecordFormats.AVRO,
      schema,
      Collections.<String, String>emptyMap()
    );
    AvroRecordFormat format = new AvroRecordFormat();
    format.initialize(formatSpecification);

    GenericRecord actual = format.read(ByteBuffer.wrap(serializedRecord));
    Assert.assertEquals(Integer.MAX_VALUE, actual.get("int"));
    GenericRecord actualInner = (GenericRecord) actual.get("record");
    Assert.assertEquals(5, actualInner.get("int"));
    Assert.assertEquals(3.14159, actualInner.get("double"));
  }

  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(GSON.toJson(cdapSchema));
  }

  // needed since avro uses their own utf8 class for strings
  private void assertMapEquals(Map<String, Object> expected, Map<Object, Object> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (Map.Entry<Object, Object> entry : actual.entrySet()) {
      Assert.assertEquals(expected.get(entry.getKey().toString()), entry.getValue());
    }
  }
}
