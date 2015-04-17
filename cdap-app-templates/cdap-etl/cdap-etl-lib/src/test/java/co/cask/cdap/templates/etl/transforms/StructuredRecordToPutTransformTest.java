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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformContext;
import co.cask.cdap.templates.etl.common.MockEmitter;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public class StructuredRecordToPutTransformTest {
  
  @Test(expected = IllegalArgumentException.class)
  public void testNullRowkeyThrowsException() throws Exception {
    // initialize transform
    Transform transform = new StructuredRecordToPutTransform();
    TransformContext context = new MockTransformContext(ImmutableMap.of("row.field", "key"));
    transform.initialize(context);

    Schema schema = Schema.recordOf("record", Schema.Field.of("key", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord record = StructuredRecord.builder(schema).build();

    MockEmitter<Put> emitter = new MockEmitter<Put>();
    transform.transform(record, emitter);
  }

  @Test
  public void testNullableFields() throws Exception {
    // initialize the transform
    Transform transform = new StructuredRecordToPutTransform();
    TransformContext context = new MockTransformContext(ImmutableMap.of("row.field", "key"));
    transform.initialize(context);

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("key", Schema.of(Schema.Type.INT)),
      Schema.Field.of("nullable", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("non_nullable", Schema.of(Schema.Type.STRING))
    );

    // valid record
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("key", 1)
      .set("non_nullable", "foo")
      .build();

    MockEmitter<Put> emitter = new MockEmitter<Put>();
    transform.transform(record, emitter);
    Put transformed = emitter.getEmitted().get(0);

    Assert.assertEquals(1, Bytes.toInt(transformed.getRow()));
    // expect a null value for the nullable field
    Assert.assertEquals(2, transformed.getValues().size());
    Assert.assertEquals("foo", Bytes.toString(transformed.getValues().get(Bytes.toBytes("non_nullable"))));
    Assert.assertNull(transformed.getValues().get(Bytes.toBytes("nullable")));
  }

  @Test
  public void testTransform() throws Exception {
    // initialize the transform
    Transform transform = new StructuredRecordToPutTransform();
    TransformContext context = new MockTransformContext(ImmutableMap.of("row.field", "stringField"));
    transform.initialize(context);

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("boolField", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("intField", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("longField", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("floatField", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("doubleField", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("bytesField", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("boolField", true)
      .set("intField", 5)
      .set("longField", 10L)
      .set("floatField", 3.14f)
      .set("doubleField", 3.14)
      .set("bytesField", Bytes.toBytes("foo"))
      .set("stringField", "key")
      .build();

    MockEmitter<Put> emitter = new MockEmitter<Put>();
    transform.transform(record, emitter);
    Put transformed = emitter.getEmitted().get(0);

    Assert.assertEquals("key", Bytes.toString(transformed.getRow()));
    Map<byte[], byte[]> values = transformed.getValues();
    Assert.assertTrue(Bytes.toBoolean(values.get(Bytes.toBytes("boolField"))));
    Assert.assertEquals(5, Bytes.toInt(values.get(Bytes.toBytes("intField"))));
    Assert.assertEquals(10L, Bytes.toLong(values.get(Bytes.toBytes("longField"))));
    Assert.assertTrue(Math.abs(3.14f - Bytes.toFloat(values.get(Bytes.toBytes("floatField")))) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - Bytes.toDouble(values.get(Bytes.toBytes("doubleField")))) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), values.get(Bytes.toBytes("bytesField")));
  }
}
