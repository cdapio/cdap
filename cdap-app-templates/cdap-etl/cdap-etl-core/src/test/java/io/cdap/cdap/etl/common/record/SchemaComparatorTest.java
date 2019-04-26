/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.common.record;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Test Schema comparison.
 */
public class SchemaComparatorTest {
  private static final SchemaComparator COMPARATOR = new SchemaComparator();
  private static final Schema INT = Schema.of(Schema.Type.INT);
  private static final Schema LONG = Schema.of(Schema.Type.LONG);
  private static final Schema FLOAT = Schema.of(Schema.Type.FLOAT);
  private static final Schema DOUBLE = Schema.of(Schema.Type.DOUBLE);
  private static final Schema STRING = Schema.of(Schema.Type.STRING);
  private static final Schema BOOL = Schema.of(Schema.Type.BOOLEAN);
  private static final Schema BYTES = Schema.of(Schema.Type.BYTES);
  private static final Schema NULL = Schema.of(Schema.Type.NULL);
  private static final Schema ARRAY_INT = Schema.arrayOf(INT);
  private static final Schema NULLABLE_INT = Schema.nullableOf(INT);
  private static final Schema UNION_NUMERIC = Schema.unionOf(INT, LONG, FLOAT, DOUBLE);
  private static final Schema MAP_STRING_INT = Schema.mapOf(STRING, INT);
  private static final Schema RECORD_FLAT = Schema.recordOf("x",
                                                            Schema.Field.of("int", INT),
                                                            Schema.Field.of("long", LONG),
                                                            Schema.Field.of("float", FLOAT),
                                                            Schema.Field.of("double", DOUBLE),
                                                            Schema.Field.of("string", STRING),
                                                            Schema.Field.of("bool", BOOL),
                                                            Schema.Field.of("bytes", BYTES),
                                                            Schema.Field.of("null", NULL),
                                                            Schema.Field.of("arr", ARRAY_INT),
                                                            Schema.Field.of("union", UNION_NUMERIC),
                                                            Schema.Field.of("map", MAP_STRING_INT));

  @Test
  public void testTypeComparison() {
    Set<Schema> schemas = new HashSet<>(Arrays.asList(INT, LONG, FLOAT, DOUBLE, STRING, BOOL, BYTES, NULL,
                                                      ARRAY_INT, NULLABLE_INT, UNION_NUMERIC,
                                                      MAP_STRING_INT, RECORD_FLAT));

    for (Schema schema1 : schemas) {
      Assert.assertEquals(0, COMPARATOR.compare(schema1, schema1));
      Set<Schema> otherSchemas = new HashSet<>(schemas);
      otherSchemas.remove(schema1);
      for (Schema schema2 : otherSchemas) {
        assertNotEqual(schema1, schema2);
      }
    }
  }

  @Test
  public void testNullableIs() {
    assertNotEqual(INT, NULLABLE_INT);
  }

  @Test
  public void testUnionSubset() {
    assertNotEqual(Schema.unionOf(INT, LONG, NULL), Schema.unionOf(INT, LONG));
  }

  @Test
  public void testSimpleArrayComponents() {
    assertNotEqual(Schema.arrayOf(INT), Schema.arrayOf(LONG));
    assertNotEqual(Schema.arrayOf(INT), Schema.arrayOf(NULLABLE_INT));
  }

  @Test
  public void testNestedArrayComponents() {
    assertNotEqual(Schema.arrayOf(RECORD_FLAT), Schema.arrayOf(Schema.recordOf("x", Schema.Field.of("x", INT))));
  }

  @Test
  public void testRecordFieldOrder() {
    Schema r1 = Schema.recordOf("x", Schema.Field.of("x", INT), Schema.Field.of("y", INT));
    Schema r2 = Schema.recordOf("x", Schema.Field.of("y", INT), Schema.Field.of("x", INT));
    assertNotEqual(r1, r2);
  }

  @Test
  public void testRecordSubset() {
    Schema r1 = Schema.recordOf("x", Schema.Field.of("x", INT), Schema.Field.of("y", INT));
    Schema r2 = Schema.recordOf("x", Schema.Field.of("x", INT));
    assertNotEqual(r1, r2);
  }

  @Test
  public void testRecordName() {
    assertNotEqual(Schema.recordOf("x", Schema.Field.of("x", INT)),
                   Schema.recordOf("y", Schema.Field.of("x", INT)));
  }

  @Test
  public void testRecordFieldName() {
    assertNotEqual(Schema.recordOf("x", Schema.Field.of("x", INT)),
                   Schema.recordOf("x", Schema.Field.of("y", INT)));
  }

  @Test
  public void testNestedRecord() {
    Schema middle = Schema.recordOf("middle",
                                    Schema.Field.of("int", INT),
                                    Schema.Field.of("inner", RECORD_FLAT));
    Schema outer1 = Schema.recordOf("outer",
                                    Schema.Field.of("middle", middle));
    Schema outer2 = Schema.recordOf("outer",
                                    Schema.Field.of("middle", RECORD_FLAT));
    assertNotEqual(outer1, outer2);
    Assert.assertEquals(0, COMPARATOR.compare(outer1, outer1));
    Assert.assertEquals(0, COMPARATOR.compare(outer2, outer2));
  }

  @Test
  public void testSimpleMap() {
    Assert.assertNotEquals(0, COMPARATOR.compare(Schema.mapOf(STRING, STRING),
                                                 Schema.mapOf(STRING, INT)));
  }

  @Test
  public void testNestedMapValues() {
    Assert.assertNotEquals(0, COMPARATOR.compare(
      Schema.mapOf(STRING, RECORD_FLAT), Schema.mapOf(STRING, Schema.recordOf("x", Schema.Field.of("x", INT)))));
  }

  @Test
  public void testSamePhysicalDifferentLogicalTypes() {
    Assert.assertNotEquals(0, COMPARATOR.compare(Schema.of(Schema.LogicalType.DATE),
                                                 Schema.of(Schema.LogicalType.TIME_MILLIS)));
    Assert.assertNotEquals(0, COMPARATOR.compare(Schema.of(Schema.LogicalType.TIME_MICROS),
                                                 Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));
  }

  private void assertNotEqual(Schema s1, Schema s2) {
    int comp1 = COMPARATOR.compare(s1, s2);
    int comp2 = COMPARATOR.compare(s2, s1);
    Assert.assertNotEquals(0, comp1);
    Assert.assertNotEquals(0, comp2);
    if (comp1 > 0) {
      Assert.assertTrue(comp2 < 0);
    } else {
      Assert.assertTrue(comp2 > 0);
    }
  }
}
