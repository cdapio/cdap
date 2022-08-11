/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.api.data.schema;

import io.cdap.cdap.api.data.schema.Schema.Field;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;


/**
 * Test Schema Class
 */
public class SchemaTest {

  private Schema createNamelessRecord1() {
    Field stringField = Field.of("stringField", Schema.of(Schema.Type.STRING));
    Field longField = Field.of("numericField", Schema.of(Schema.Type.LONG));
    Field booleanField = Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN));
    Field fixedField = Field.of("fixedField", Schema.fixedOf(ByteBuffer.wrap("testFixed".getBytes()), "fixedName"));
    Field namelessRecordField = Field.of("namelessRecord", Schema.recordOf(booleanField));
    Field recordField = Field.of("namedRecord", Schema.recordOf("namedRecord", stringField, longField, fixedField));
    return Schema.recordOf(recordField, namelessRecordField);
  }

  private Schema createNamelessRecord2() {
    Field stringField = Field.of("stringField", Schema.of(Schema.Type.STRING));
    Field booleanField = Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN));
    Field namelessRecordField = Field.of("namelessRecord", Schema.recordOf(booleanField));
    Field fixedField = Field.of("fixedField", Schema.fixedOf(ByteBuffer.wrap("testFixed".getBytes()), "fixedName"));
    Field recordField = Field.of("namedRecord", Schema.recordOf("namedRecord", stringField, fixedField));
    return Schema.recordOf(recordField, namelessRecordField);
  }

  /**
   * Checks if two nameless records with the same field have the same hashes and names
   */
  @Test
  public void test_SameHashNamelessRecords() {
    Schema namelessRecord1 = createNamelessRecord1();
    Schema namelessRecord2 = createNamelessRecord1();
    Assert.assertEquals(namelessRecord1.getRecordName(), namelessRecord2.getRecordName());
    Assert.assertEquals(namelessRecord1, namelessRecord2);
  }


  /**
   * Checks if two nameless records with different field have different hashes and names
   */
  @Test
  public void test_DifferentHashNamelessRecords() {
    Schema namelessRecord1 = createNamelessRecord1();
    Schema namelessRecord2 = createNamelessRecord2();
    Assert.assertNotEquals(namelessRecord1.getRecordName(), namelessRecord2.getRecordName());
    Assert.assertNotEquals(namelessRecord1, namelessRecord2);
  }

}
