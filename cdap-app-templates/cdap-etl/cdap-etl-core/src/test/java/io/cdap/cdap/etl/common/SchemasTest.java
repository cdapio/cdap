/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link Schema}
 */
public class SchemasTest {

  @Test
  public void testEqualsIgnoringRecordNameFlat() {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of("b", Schema.of(Schema.Type.BOOLEAN)));
    Schema s1 = Schema.recordOf("s1", fields);
    Schema s2 = Schema.recordOf("s2", fields);
    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(Schemas.equalsIgnoringRecordName(s1, s2));
  }

  @Test
  public void testEqualsIgnoringRecordNameInsideRecord() {
    Schema inner1 = Schema.recordOf("inner1", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Schema inner2 = Schema.recordOf("inner2", Schema.Field.of("x", Schema.of(Schema.Type.INT)));

    Schema outer1 = Schema.recordOf("outer1", Schema.Field.of("inner", inner1));
    Schema outer2 = Schema.recordOf("outer2", Schema.Field.of("inner", inner2));
    Assert.assertNotEquals(outer1, outer2);
    Assert.assertTrue(Schemas.equalsIgnoringRecordName(outer1, outer2));
  }

  @Test
  public void testEqualsIgnoringRecordNameInsideUnion() {
    Schema inner1 = Schema.recordOf("inner1", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Schema inner2 = Schema.recordOf("inner2", Schema.Field.of("x", Schema.of(Schema.Type.INT)));

    Schema s1 = Schema.recordOf("s1", Schema.Field.of("u", Schema.unionOf(inner1, Schema.of(Schema.Type.NULL))));
    Schema s2 = Schema.recordOf("s2", Schema.Field.of("u", Schema.unionOf(inner2, Schema.of(Schema.Type.NULL))));
    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(Schemas.equalsIgnoringRecordName(s1, s2));

    Schema inner3 = Schema.recordOf("inner3", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Schema s3 = Schema.recordOf("s3", Schema.Field.of("u", Schema.unionOf(inner3, Schema.of(Schema.Type.NULL))));
    Assert.assertFalse(Schemas.equalsIgnoringRecordName(s1, s3));
  }

  @Test
  public void testEqualsIgnoringRecordNameInsideMap() {
    Schema inner1 = Schema.recordOf("inner1", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Schema inner2 = Schema.recordOf("inner2", Schema.Field.of("x", Schema.of(Schema.Type.INT)));

    Schema s1 = Schema.recordOf("s1", Schema.Field.of("u", Schema.mapOf(Schema.of(Schema.Type.STRING), inner1)));
    Schema s2 = Schema.recordOf("s2", Schema.Field.of("u", Schema.mapOf(Schema.of(Schema.Type.STRING), inner2)));
    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(Schemas.equalsIgnoringRecordName(s1, s2));

    Schema inner3 = Schema.recordOf("inner3", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Schema s3 = Schema.recordOf("s3", Schema.Field.of("u", Schema.mapOf(Schema.of(Schema.Type.STRING), inner3)));
    Assert.assertFalse(Schemas.equalsIgnoringRecordName(s1, s3));
  }

  @Test
  public void testEqualsIgnoringRecordNameInsideArray() {
    Schema inner1 = Schema.recordOf("inner1", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    Schema inner2 = Schema.recordOf("inner2", Schema.Field.of("x", Schema.of(Schema.Type.INT)));

    Schema s1 = Schema.recordOf("s1", Schema.Field.of("u", Schema.arrayOf(inner1)));
    Schema s2 = Schema.recordOf("s2", Schema.Field.of("u", Schema.arrayOf(inner2)));
    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(Schemas.equalsIgnoringRecordName(s1, s2));

    Schema inner3 = Schema.recordOf("inner3", Schema.Field.of("x", Schema.of(Schema.Type.LONG)));
    Schema s3 = Schema.recordOf("s3", Schema.Field.of("u", Schema.arrayOf(inner3)));
    Assert.assertFalse(Schemas.equalsIgnoringRecordName(s1, s3));
  }
}
