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

package co.cask.cdap.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link StructuredRecordStringConverter} class from {@link StructuredRecord} to json string
 */
@SuppressWarnings("unchecked")
public class StructuredRecordStringConverterTest {
  Schema schema;

  @Test
  public void checkConversion() throws Exception {
    StructuredRecord initial = getStructuredRecord();
    String jsonOfRecord = StructuredRecordStringConverter.toJsonString(initial);
    StructuredRecord recordOfJson = StructuredRecordStringConverter.fromJsonString(jsonOfRecord, schema);
    assertRecordsEqual(initial, recordOfJson);
  }

  private StructuredRecord getStructuredRecord() {
    Schema.Field mapField = Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                    Schema.of(Schema.Type.STRING)));
    Schema.Field idField = Schema.Field.of("id", Schema.of(Schema.Type.INT));
    Schema.Field nameField = Schema.Field.of("name", Schema.of(Schema.Type.STRING));
    Schema.Field scoreField = Schema.Field.of("score", Schema.of(Schema.Type.DOUBLE));
    Schema.Field graduatedField = Schema.Field.of("graduated", Schema.of(Schema.Type.BOOLEAN));
    Schema.Field binaryNameField = Schema.Field.of("binary", Schema.of(Schema.Type.BYTES));
    Schema.Field timeField = Schema.Field.of("time", Schema.of(Schema.Type.LONG));
    Schema.Field recordField = Schema.Field.of("innerRecord", Schema.recordOf("innerSchema", mapField, idField));
    StructuredRecord.Builder innerRecordBuilder = StructuredRecord.builder(Schema.recordOf("innerSchema",
                                                                                           mapField, idField));
    innerRecordBuilder
      .set("headers", ImmutableMap.of("a1", "a2"))
      .set("id", 3);

    schema = Schema.recordOf("complexRecord", mapField, idField, nameField, scoreField,
                             graduatedField, binaryNameField, timeField, recordField);

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    recordBuilder
      .set("headers", ImmutableMap.of("h1", "v1"))
      .set("id", 1)
      .set("name", "Bob").
      set("score", 3.4)
      .set("graduated", false)
      .set("time", System.currentTimeMillis())
      .set("binary", "Bob".getBytes(Charsets.UTF_8))
      .set("innerRecord", innerRecordBuilder.build());

    return recordBuilder.build();
  }

  private void assertRecordsEqual(StructuredRecord one, StructuredRecord two) {
    Assert.assertTrue(one.getSchema().getRecordName().equals(two.getSchema().getRecordName()));
    Assert.assertTrue(one.getSchema().getFields().size() == two.getSchema().getFields().size());
    for (Schema.Field field : one.getSchema().getFields()) {
      if (one.get(field.getName()).getClass().equals(StructuredRecord.class)) {
        assertRecordsEqual((StructuredRecord) one.get(field.getName()), (StructuredRecord) two.get(field.getName()));
      } else if (field.getName().equals("binary")) {
        Assert.assertArrayEquals((byte[]) two.get(field.getName()), (byte[]) one.get(field.getName()));
      } else {
        Assert.assertTrue(one.get(field.getName()).toString().equals(two.get(field.getName()).toString()));
      }
    }
  }
}
