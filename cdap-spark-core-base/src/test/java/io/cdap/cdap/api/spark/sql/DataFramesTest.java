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

package co.cask.cdap.api.spark.sql;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Unit test for {@link DataFrames} class.
 */
public class DataFramesTest {

  @Test
  public void testSimpleType() {
    for (Schema.Type type : Schema.Type.values()) {
      if (type.isSimpleType()) {
        DataType dataType = DataFrames.toDataType(Schema.of(type));
        switch (type) {
          case NULL:
            Assert.assertEquals(DataTypes.NullType, dataType);
            break;
          case BOOLEAN:
            Assert.assertEquals(DataTypes.BooleanType, dataType);
            break;
          case INT:
            Assert.assertEquals(DataTypes.IntegerType, dataType);
            break;
          case LONG:
            Assert.assertEquals(DataTypes.LongType, dataType);
            break;
          case FLOAT:
            Assert.assertEquals(DataTypes.FloatType, dataType);
            break;
          case DOUBLE:
            Assert.assertEquals(DataTypes.DoubleType, dataType);
            break;
          case BYTES:
            Assert.assertEquals(DataTypes.BinaryType, dataType);
            break;
          case STRING:
            Assert.assertEquals(DataTypes.StringType, dataType);
            break;
          default:
            // Should not happen
            Assert.fail("Only testing simple type");
        }

        // Conversion should work both ways
        Assert.assertEquals(type, DataFrames.toSchema(dataType).getType());
      }
    }
  }

  @Test
  public void testArrayType() {
    // Simple array
    Schema schema = Schema.arrayOf(Schema.of(Schema.Type.INT));
    ArrayType dataType = DataFrames.toDataType(schema);
    Assert.assertFalse(dataType.containsNull());
    Assert.assertEquals(DataTypes.IntegerType, dataType.elementType());
    Assert.assertEquals(schema, DataFrames.toSchema(dataType));

    // Array with nullable element
    schema = Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    dataType = DataFrames.toDataType(schema);
    Assert.assertTrue(dataType.containsNull());
    Assert.assertEquals(DataTypes.StringType, dataType.elementType());
    Assert.assertEquals(schema, DataFrames.toSchema(dataType));

    // Byte array special case
    dataType = ArrayType.apply(DataTypes.ByteType);
    Assert.assertEquals(Schema.of(Schema.Type.BYTES), DataFrames.toSchema(dataType));
  }

  @Test
  public void testMapType() {
    // Simple Map
    Schema schema = Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.BOOLEAN));
    MapType dataType = DataFrames.toDataType(schema);
    Assert.assertFalse(dataType.valueContainsNull());
    Assert.assertEquals(DataTypes.StringType, dataType.keyType());
    Assert.assertEquals(DataTypes.BooleanType, dataType.valueType());
    Assert.assertEquals(schema, DataFrames.toSchema(dataType));

    // Map with nullable value
    schema = Schema.mapOf(Schema.of(Schema.Type.INT), Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    dataType = DataFrames.toDataType(schema);
    Assert.assertTrue(dataType.valueContainsNull());
    Assert.assertEquals(DataTypes.IntegerType, dataType.keyType());
    Assert.assertEquals(DataTypes.StringType, dataType.valueType());
    Assert.assertEquals(schema, DataFrames.toSchema(dataType));
  }

  @Test
  public void testStructType() {
    Schema schema = Schema.recordOf("Record0",
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("age", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("profile", Schema.nullableOf(
                                                                Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                             Schema.of(Schema.Type.STRING)))));
    StructType dataType = DataFrames.toDataType(schema);
    Assert.assertEquals(DataTypes.StringType, dataType.apply("name").dataType());
    Assert.assertFalse(dataType.apply("name").nullable());

    Assert.assertEquals(DataTypes.IntegerType, dataType.apply("age").dataType());
    Assert.assertFalse(dataType.apply("age").nullable());

    Assert.assertTrue(dataType.apply("profile").dataType() instanceof MapType);
    Assert.assertTrue(dataType.apply("profile").nullable());

    Assert.assertEquals(schema, DataFrames.toSchema(dataType));
  }

  @Test
  public void testRowConversion() {
    Schema schema = Schema.recordOf(
      "Record0",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("nullableField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("arrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("mapField",
                      Schema.nullableOf(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                     Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
      Schema.Field.of("recordField",
                      Schema.recordOf("Record1",
                                      Schema.Field.of("arrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING)))))
    );
    StructType dataType = DataFrames.toDataType(schema);

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("booleanField", true)
      .set("intField", 123)
      .set("longField", 456L)
      .set("floatField", 1.2f)
      .set("doubleField", 2.5d)
      .set("stringField", "hello")
      .set("bytesField", new byte[]{1, 2, 3})
      .set("nullableField", null)
      .set("arrayField", Arrays.asList("1", "2", "3"))
      .set("mapField", Collections.singletonMap("k", "v"))
      .set("recordField",
           StructuredRecord.builder(schema.getField("recordField").getSchema())
             .set("arrayField", new String[]{"a", "b", "c"}).build())
      .build();

    Row row = DataFrames.toRow(record, dataType);

    Assert.assertTrue(row.getBoolean(0));
    Assert.assertEquals(123, row.getInt(1));
    Assert.assertEquals(456L, row.getLong(2));
    Assert.assertEquals(1.2f, row.getFloat(3), 0.00001f);
    Assert.assertEquals(2.5d, row.getDouble(4), 0.00001d);
    Assert.assertEquals("hello", row.getString(5));
    Assert.assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) row.get(6));
    Assert.assertTrue(row.isNullAt(7));
    Assert.assertEquals(Arrays.asList("1", "2", "3"), row.getList(8));
    Assert.assertEquals(Collections.singletonMap("k", "v"), row.getJavaMap(9));

    Row fieldRow = row.getAs(10);
    Assert.assertEquals(Arrays.asList("a", "b", "c"), fieldRow.getList(0));
  }

  @Test
  public void testRecordConversion() {
    Row row = RowFactory.create(
      true,
      10,
      20L,
      30f,
      40d,
      "String",
      new byte[] {1, 2, 3},
      null,
      JavaConversions.asScalaBuffer(Arrays.asList("1", "2", "3")).toSeq(),
      JavaConversions.mapAsScalaMap(Collections.singletonMap("k", "v")),
      RowFactory.create(JavaConversions.asScalaBuffer(Arrays.asList("a", "b", "c", null)))
    );

    Schema schema = Schema.recordOf(
      "Record0",
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("nullableField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("arrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("mapField",
                      Schema.nullableOf(Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                     Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
      Schema.Field.of("recordField",
        Schema.recordOf("Record1",
                        Schema.Field.of("array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))))
    );

    StructuredRecord record = DataFrames.fromRow(row, schema);

    Assert.assertTrue(record.<Boolean>get("booleanField"));
    Assert.assertEquals(10, record.<Integer>get("intField").intValue());
    Assert.assertEquals(20L, record.<Long>get("longField").longValue());
    Assert.assertEquals(30f, record.<Float>get("floatField").floatValue(), 0.0001f);
    Assert.assertEquals(40d, record.<Double>get("doubleField").doubleValue(), 0.0001d);
    Assert.assertEquals("String", record.<String>get("stringField"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), record.<ByteBuffer>get("bytesField"));
    Assert.assertNull(record.get("nullableField"));
    Assert.assertEquals(Arrays.asList("1", "2", "3"), record.<Collection<String>>get("arrayField"));
    Assert.assertEquals(Collections.singletonMap("k", "v"), record.<Map<String, String>>get("mapField"));
    Assert.assertEquals(Arrays.asList("a", "b", "c", null),
                        record.<StructuredRecord>get("recordField").<Collection<String>>get("array"));
  }
}
