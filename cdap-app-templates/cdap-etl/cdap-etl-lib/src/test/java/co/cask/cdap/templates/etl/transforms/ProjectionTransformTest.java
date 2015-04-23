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
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.common.MockEmitter;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ProjectionTransformTest {
  private static final Schema SIMPLE_TYPES_SCHEMA = Schema.recordOf("record",
    Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
    Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));
  private static final StructuredRecord SIMPLE_TYPES_RECORD = StructuredRecord.builder(SIMPLE_TYPES_SCHEMA)
    .set("booleanField", true)
    .set("intField", 28)
    .set("longField", 99L)
    .set("floatField", 2.71f)
    .set("doubleField", 3.14)
    .set("bytesField", Bytes.toBytes("foo"))
    .set("stringField", "bar")
    .build();

  @Test(expected = IllegalArgumentException.class)
  public void testSameFieldMultipleConverts() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of("convert", "x:int,x:long"));
    transform.initialize(transformContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSameFieldMultipleRenames() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of("rename", "x:z,x:y"));
    transform.initialize(transformContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleRenamesToSameField() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of("rename", "x:z,y:z"));
    transform.initialize(transformContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSyntax() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of("rename", "x,y"));
    transform.initialize(transformContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConversion() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of("convert", "x:int"));
    transform.initialize(transformContext);

    Schema schema = Schema.recordOf("record", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    StructuredRecord input = StructuredRecord.builder(schema).set("x", 5L).build();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
  }

  @Test
  public void testDropFields() throws Exception {
    Schema schema = Schema.recordOf("three",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("z", Schema.arrayOf(Schema.of(Schema.Type.INT))));
    StructuredRecord input = StructuredRecord.builder(schema)
      .set("x", 1)
      .set("y", 3.14)
      .set("z", new int[] { 1, 2, 3 })
      .build();
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(
      ImmutableMap.of("drop", "y, z"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("three.projected", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertEquals(1, output.get("x"));
  }

  @Test
  public void testRenameFields() throws Exception {
    Schema schema = Schema.recordOf("three",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("z", Schema.arrayOf(Schema.of(Schema.Type.INT))));
    StructuredRecord input = StructuredRecord.builder(schema)
      .set("x", 1)
      .set("y", 3.14)
      .set("z", new int[] { 1, 2, 3 })
      .build();
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(
      ImmutableMap.of("rename", "x:y,y:z,z:x"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("three",
      Schema.Field.of("y", Schema.of(Schema.Type.INT)),
      Schema.Field.of("z", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("x", Schema.arrayOf(Schema.of(Schema.Type.INT))));
    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertEquals(1, output.get("y"));
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("z")) < 0.000001);
    Assert.assertArrayEquals(new int[]{1, 2, 3}, (int[]) output.get("x"));
  }

  @Test
  public void testDropRenameConvert() throws Exception {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("x", Schema.of(Schema.Type.INT)),
      Schema.Field.of("y", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    StructuredRecord input = StructuredRecord.builder(schema)
      .set("x", 5)
      .set("y", 10)
      .build();

    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "drop", "x", "rename", "y:x", "convert", "y:string"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("record.projected",
      Schema.Field.of("x", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertEquals("10", output.get("x"));
  }

  @Test
  public void testConvertToString() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert",
      "booleanField:string,intField:string,longField:string," +
        "floatField:string,doubleField:string,bytesField:string,stringField:string"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(SIMPLE_TYPES_RECORD, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("record",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("intField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("longField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));
    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertEquals("true", output.get("booleanField"));
    Assert.assertEquals("28", output.get("intField"));
    Assert.assertEquals("99", output.get("longField"));
    Assert.assertEquals("2.71", output.get("floatField"));
    Assert.assertEquals("3.14", output.get("doubleField"));
    Assert.assertEquals("foo", output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
  }

  @Test
  public void testConvertFromString() throws Exception {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("intField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("longField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert",
      "booleanField:boolean,intField:int,longField:long," +
        "floatField:float,doubleField:double,bytesField:bytes,stringField:string"));
    transform.initialize(transformContext);

    StructuredRecord input = StructuredRecord.builder(schema)
      .set("booleanField", "true")
      .set("intField", "28")
      .set("longField", "99")
      .set("floatField", "2.71")
      .set("doubleField", "3.14")
      .set("bytesField", "foo")
      .set("stringField", "bar")
      .build();

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Assert.assertEquals(SIMPLE_TYPES_SCHEMA, output.getSchema());
    Assert.assertTrue((Boolean) output.get("booleanField"));
    Assert.assertEquals(28, output.get("intField"));
    Assert.assertEquals(99L, output.get("longField"));
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
  }

  @Test
  public void testConvertToBytes() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert",
      "booleanField:bytes,intField:bytes,longField:bytes," +
        "floatField:bytes,doubleField:bytes,bytesField:bytes,stringField:bytes"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(SIMPLE_TYPES_RECORD, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("record",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("intField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("longField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.BYTES)));
    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertArrayEquals(Bytes.toBytes(true), (byte[]) output.get("booleanField"));
    Assert.assertArrayEquals(Bytes.toBytes(28), (byte[]) output.get("intField"));
    Assert.assertArrayEquals(Bytes.toBytes(99L), (byte[]) output.get("longField"));
    Assert.assertArrayEquals(Bytes.toBytes(2.71f), (byte[]) output.get("floatField"));
    Assert.assertArrayEquals(Bytes.toBytes(3.14), (byte[]) output.get("doubleField"));
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
    Assert.assertArrayEquals(Bytes.toBytes("bar"), (byte[]) output.get("stringField"));
  }

  @Test
  public void testConvertFromBytes() throws Exception {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("intField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("longField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.BYTES)));
    StructuredRecord input = StructuredRecord.builder(schema)
      .set("booleanField", Bytes.toBytes(true))
      .set("intField", Bytes.toBytes(28))
      .set("longField", Bytes.toBytes(99L))
      .set("floatField", Bytes.toBytes(2.71f))
      .set("doubleField", Bytes.toBytes(3.14))
      .set("bytesField", Bytes.toBytes("foo"))
      .set("stringField", Bytes.toBytes("bar"))
      .build();

    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert",
      "booleanField:boolean,intField:int,longField:long," +
        "floatField:float,doubleField:double,bytesField:bytes,stringField:string"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(input, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Assert.assertEquals(SIMPLE_TYPES_SCHEMA, output.getSchema());
    Assert.assertTrue((Boolean) output.get("booleanField"));
    Assert.assertEquals(28, output.get("intField"));
    Assert.assertEquals(99L, output.get("longField"));
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
  }

  @Test
  public void testConvertToLong() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert", "intField:long"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(SIMPLE_TYPES_RECORD, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("record.projected",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("intField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));

    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertTrue((Boolean) output.get("booleanField"));
    Assert.assertEquals(28L, output.get("intField"));
    Assert.assertEquals(99L, output.get("longField"));
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
  }

  @Test
  public void testConvertToFloat() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert", "intField:float,longField:float"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(SIMPLE_TYPES_RECORD, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("record.projected",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("intField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));

    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertTrue((Boolean) output.get("booleanField"));
    Assert.assertEquals(28f, output.get("intField"));
    Assert.assertEquals(99f, output.get("longField"));
    Assert.assertTrue(Math.abs(2.71f - (Float) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
  }

  @Test
  public void testConvertToDouble() throws Exception {
    TransformStage<StructuredRecord, StructuredRecord> transform = new ProjectionTransform();
    StageContext transformContext = new MockStageContext(ImmutableMap.of(
      "convert", "intField:double,longField:double,floatField:double"));
    transform.initialize(transformContext);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<StructuredRecord>();
    transform.transform(SIMPLE_TYPES_RECORD, emitter);
    StructuredRecord output = emitter.getEmitted().get(0);

    Schema expectedSchema = Schema.recordOf("record.projected",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("intField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("longField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)));

    Assert.assertEquals(expectedSchema, output.getSchema());
    Assert.assertTrue((Boolean) output.get("booleanField"));
    Assert.assertEquals(28d, output.get("intField"));
    Assert.assertEquals(99d, output.get("longField"));
    Assert.assertTrue(Math.abs(2.71 - (Double) output.get("floatField")) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) output.get("doubleField")) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes("foo"), (byte[]) output.get("bytesField"));
    Assert.assertEquals("bar", output.get("stringField"));
  }
}
