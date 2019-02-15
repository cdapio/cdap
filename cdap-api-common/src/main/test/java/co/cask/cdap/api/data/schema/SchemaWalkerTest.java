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

package co.cask.cdap.api.data.schema;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tests the schema walker.
 */
public class SchemaWalkerTest {

  @Test
  public void testSchemaWalker() {
    Schema bytesArraySchema = Schema.arrayOf(Schema.of(Schema.Type.BYTES));
    Schema stringArraySchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));
    Schema booleanBytesMapSchema = Schema.mapOf(Schema.of(Schema.Type.BOOLEAN), Schema.of(Schema.Type.BYTES));
    Schema nestedMapSchema = Schema.mapOf(bytesArraySchema, booleanBytesMapSchema);
    Schema record22Schema = Schema.recordOf("record22", Schema.Field.of("a", nestedMapSchema));
    Schema record22ArraySchema = Schema.arrayOf(record22Schema);
    Schema bytesDoubleMapSchema = Schema.mapOf(Schema.of(Schema.Type.BYTES), Schema.of(Schema.Type.DOUBLE));
    Schema record21Schema = Schema.recordOf("record21",
                                            Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("y", stringArraySchema),
                                            Schema.Field.of("z", bytesDoubleMapSchema));
    Schema record21to22MapSchema = Schema.mapOf(record21Schema, record22ArraySchema);
    Schema nullableIntSchema = Schema.nullableOf(Schema.of(Schema.Type.INT));
    Schema tripeUnionSchema = Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.LONG),
                                             Schema.of(Schema.Type.NULL));
    Schema complexSchema = Schema.recordOf("record1",
                                           Schema.Field.of("map1", record21to22MapSchema),
                                           Schema.Field.of("i", nullableIntSchema),
                                           Schema.Field.of("j", tripeUnionSchema));
    Schema anotherComplexSchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));
    Schema superComplexSchema = Schema.unionOf(complexSchema, anotherComplexSchema, Schema.of(Schema.Type.NULL));

    List<Pair> results = new ArrayList<>();
    SchemaWalker.walk(superComplexSchema, (field, schema) -> results.add(new Pair(field, schema)));

    List<Pair> expected = new ArrayList<>();
    expected.add(new Pair(null, superComplexSchema));
    expected.add(new Pair("record1", complexSchema));
    expected.add(new Pair("map1", record21to22MapSchema));
    expected.add(new Pair("record21", record21Schema));
    expected.add(new Pair("x", Schema.of(Schema.Type.STRING)));
    expected.add(new Pair("y", stringArraySchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.STRING)));
    expected.add(new Pair("z", bytesDoubleMapSchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.BYTES)));
    expected.add(new Pair(null, Schema.of(Schema.Type.DOUBLE)));
    expected.add(new Pair(null, record22ArraySchema));
    expected.add(new Pair("record22", record22Schema));
    expected.add(new Pair("a", nestedMapSchema));
    expected.add(new Pair(null, bytesArraySchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.BYTES)));
    expected.add(new Pair(null, booleanBytesMapSchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.BOOLEAN)));
    expected.add(new Pair(null, Schema.of(Schema.Type.BYTES)));
    expected.add(new Pair("i", nullableIntSchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.INT)));
    expected.add(new Pair(null, Schema.of(Schema.Type.NULL)));
    expected.add(new Pair("j", tripeUnionSchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.INT)));
    expected.add(new Pair(null, Schema.of(Schema.Type.LONG)));
    expected.add(new Pair(null, Schema.of(Schema.Type.NULL)));
    expected.add(new Pair(null, anotherComplexSchema));
    expected.add(new Pair(null, Schema.of(Schema.Type.STRING)));
    expected.add(new Pair(null, Schema.of(Schema.Type.NULL)));

    Assert.assertEquals(expected, results);
  }

  static class Pair {
    private final String field;
    private final Schema schema;

    Pair(String field, Schema schema) {
      this.field = field;
      this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Pair pair = (Pair) o;
      return Objects.equals(field, pair.field) &&
        Objects.equals(schema, pair.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, schema);
    }
  }

}
