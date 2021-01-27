/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.io;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

public class SchemaHashTest {
  private Schema schema = Schema.recordOf(
    "union",
    Schema.Field.of("a", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.arrayOf(Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("b", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.arrayOf(Schema.of(Schema.Type.INT)))),
    Schema.Field.of("c", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.enumWith("something"))),
    Schema.Field.of("d", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("e", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.mapOf(Schema.of(Schema.Type.INT),
                                                     Schema.of(Schema.Type.LONG)))),
    Schema.Field.of("f", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("g", Schema.nullableOf(Schema.of(Schema.Type.INT))),
    Schema.Field.of("h", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  private Schema schemaWithLogicalType = Schema.recordOf(
    "union",
    Schema.Field.of("a", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.arrayOf(Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("b", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.arrayOf(Schema.of(Schema.LogicalType.DATE)))),
    Schema.Field.of("c", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.enumWith("something"))),
    Schema.Field.of("d", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("e", Schema.unionOf(Schema.of(Schema.Type.NULL),
                                        Schema.mapOf(Schema.of(Schema.LogicalType.DATE),
                                                     Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)))),
    Schema.Field.of("f", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("g", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
    Schema.Field.of("h", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))));

  @Test
  public void testDifferentSchemaHash() {
    Assert.assertNotEquals(schema.getSchemaHash(), schemaWithLogicalType.getSchemaHash());
  }

  @Test
  public void testSameSchemaHash() {
    Assert.assertEquals(schema.getSchemaHash(), schema.getSchemaHash());
    Assert.assertEquals(schemaWithLogicalType.getSchemaHash(), schemaWithLogicalType.getSchemaHash());
  }

  @Test
  public void testDecimalSchemaHash() {
    Schema schema1 = Schema.decimalOf(3, 5);
    Schema schema2 = Schema.decimalOf(3);

    Assert.assertNotEquals(schema1.getSchemaHash(), schema2.getSchemaHash());
    Assert.assertNotEquals(schema1, schema2);
    Assert.assertEquals(schema1.getSchemaHash(), schema1.getSchemaHash());
  }

  @Test
  public void testDateTimeHash() {
    //Datetime type should have a different hash than the string type
    Schema schema1 = Schema.of(Schema.LogicalType.DATETIME);
    Schema schema2 = Schema.of(Schema.Type.STRING);
    Schema schema3 = Schema.of(Schema.LogicalType.DATETIME);
    Assert.assertNotEquals(schema1.getSchemaHash(), schema2.getSchemaHash());
    Assert.assertEquals(schema1.getSchemaHash(), schema3.getSchemaHash());
  }
}
