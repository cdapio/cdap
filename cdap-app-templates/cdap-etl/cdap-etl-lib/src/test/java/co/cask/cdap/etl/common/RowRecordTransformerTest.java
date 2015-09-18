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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public class RowRecordTransformerTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBadRowFieldThrowsException() throws Exception {
    final Schema schema = Schema.recordOf("record", Schema.Field.of("intField", Schema.of(Schema.Type.INT)));

   new RowRecordTransformer(schema, "missing");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFieldTypeThrowsException() throws Exception {
    final Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("mapField", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));

    new RowRecordTransformer(schema, null);
  }

  @Test
  public void testTransform() throws Exception {
    byte[] rowKey = Bytes.toBytes(28);
    // (boolean, int, long, float, double, bytes, string)
    final Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("boolField", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("floatField", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("doubleField", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("bytesField", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("stringField", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    // can't use a hash map because we need to look up by byte[]
    Map<byte[], byte[]> inputColumns = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    inputColumns.put(Bytes.toBytes("boolField"), Bytes.toBytes(true));
    inputColumns.put(Bytes.toBytes("longField"), Bytes.toBytes(512L));
    inputColumns.put(Bytes.toBytes("floatField"), Bytes.toBytes(3.14f));
    inputColumns.put(Bytes.toBytes("bytesField"), Bytes.toBytes("foo"));
    inputColumns.put(Bytes.toBytes("stringField"), Bytes.toBytes("rock"));
    // include some extra columns, they shouldn't show up in the result
    inputColumns.put(Bytes.toBytes("extraField"), Bytes.toBytes("bar"));
    Row input = new Result(rowKey, inputColumns);

    RowRecordTransformer transformer = new RowRecordTransformer(schema, "intField");

    StructuredRecord actual = transformer.toRecord(input);
    Assert.assertTrue((Boolean) actual.get("boolField"));
    Assert.assertEquals(512L, actual.get("longField"));
    Assert.assertTrue(Math.abs(3.14f - (Float) actual.get("floatField")) < 0.000001);
    Assert.assertEquals("foo", Bytes.toString((byte[]) actual.get("bytesField")));
    Assert.assertEquals("rock", actual.get("stringField"));
    Assert.assertNull(actual.get("extraField"));
    // this was a nullable field and no data was set for it
    Assert.assertNull(actual.get("doubleField"));
  }
}
