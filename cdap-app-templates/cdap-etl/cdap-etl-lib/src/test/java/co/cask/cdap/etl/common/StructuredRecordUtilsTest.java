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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for {@link StructuredRecordUtils}.
 */
public class StructuredRecordUtilsTest {

  @Test
  public void testLowerAndUpperCaseTransformation() throws Exception {
    StructuredRecord record = StructuredRecord.builder(
      Schema.recordOf("dbrecord",
                      Schema.Field.of("Name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("Age", Schema.of(Schema.Type.INT)))).set("Name", "Abcd").set("Age", 10).build();
    StructuredRecord upperCaseRecord = StructuredRecordUtils.convertCase(
      record, FieldCase.toFieldCase("upPer"));
    Assert.assertEquals("Abcd", upperCaseRecord.get("NAME"));
    Assert.assertEquals(10, upperCaseRecord.get("AGE"));
    Assert.assertNull(upperCaseRecord.get("Age"));
    Assert.assertNull(upperCaseRecord.get("Name"));

    StructuredRecord lowerCaseRecord = StructuredRecordUtils.convertCase(
      record, FieldCase.toFieldCase("lowEr"));
    Assert.assertEquals("Abcd", lowerCaseRecord.get("name"));
    Assert.assertEquals(10, lowerCaseRecord.get("age"));
    Assert.assertNull(upperCaseRecord.get("Age"));
    Assert.assertNull(upperCaseRecord.get("Name"));

    StructuredRecord noChangeRecord = StructuredRecordUtils.convertCase(
      record, FieldCase.toFieldCase("no change"));
    Assert.assertEquals("Abcd", noChangeRecord.get("Name"));
    Assert.assertEquals(10, noChangeRecord.get("Age"));
  }

  @Test
  public void testInvalidTransformation() throws Exception {
    StructuredRecord record = StructuredRecord.builder(
      Schema.recordOf("dbrecord",
                      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("Age", Schema.of(Schema.Type.INT)))).set("age", "10").set("Age", 10).build();

    try {
      StructuredRecordUtils.convertCase(record, FieldCase.toFieldCase("lower"));
      Assert.fail();
    } catch (Exception e) {
      //expected
    }
  }
}
