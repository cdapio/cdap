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

package co.cask.cdap.app.runtime.spark.serializer;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for various Kryo serializers in CDAP.
 */
public class KryoSerializerTest {

  @Test
  public void testSchemaSerializer() {
    Schema schema = createSchema();

    Kryo kryo = new Kryo();
    kryo.addDefaultSerializer(Schema.class, SchemaSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, schema);
    }

    Input input = new Input(bos.toByteArray());
    Schema newSchema = kryo.readObject(input, Schema.class);

    Assert.assertEquals(schema, newSchema);
  }

  @Test
  public void testStructuredRecordSerializer() throws IOException {
    Schema schema = createSchema();

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("boolean", true)
      .set("int", 10)
      .set("long", 1L + Integer.MAX_VALUE)
      .set("float", 1.5f)
      .set("double", 2.25d)
      .set("string", "Hello World")
      .set("bytes", "Hello Bytes".getBytes(StandardCharsets.UTF_8))
      .set("enum", "a")
      .set("array", new int[]{1, 2, 3})
      .set("map", ImmutableMap.of("1", 1, "2", 2, "3", 3))
      .set("union", null).build();

    Kryo kryo = new Kryo();
    kryo.addDefaultSerializer(Schema.class, SchemaSerializer.class);
    kryo.addDefaultSerializer(StructuredRecord.class, StructuredRecordSerializer.class);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (Output output = new Output(bos)) {
      kryo.writeObject(output, record);
    }

    Input input = new Input(bos.toByteArray());
    StructuredRecord newRecord = kryo.readObject(input, StructuredRecord.class);

    // The StructuredRecord.equals is broken, Json it and compare for now
    Assert.assertEquals(StructuredRecordStringConverter.toJsonString(record),
                        StructuredRecordStringConverter.toJsonString(newRecord));
  }

  private Schema createSchema() {
    return Schema.recordOf("record",
      Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("enum", Schema.enumWith("a", "b", "c")),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("map", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.INT))),
      Schema.Field.of("union", Schema.unionOf(Schema.of(Schema.Type.NULL), Schema.of(Schema.Type.STRING)))
    );
  }
}
