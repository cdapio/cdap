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

package io.cdap.cdap.spi.data.table.field;

import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 *
 */
public class FieldValidatorTest {
  private static final StructuredTableId SIMPLE_TABLE = new StructuredTableId("simpleTable");
  private static final String KEY = "key";
  private static final String KEY2 = "key2";
  private static final String KEY3 = "key3";
  private static final String STRING_COL = "col1";

  private static StructuredTableSchema schema;

  @BeforeClass
  public static void init() {
    StructuredTableSpecification spec = new StructuredTableSpecification.Builder()
      .withId(SIMPLE_TABLE)
      .withFields(Fields.intType(KEY), Fields.longType(KEY2), Fields.stringType(KEY3), Fields.stringType(STRING_COL))
      .withPrimaryKeys(KEY, KEY2, KEY3)
      .build();
    schema = new StructuredTableSchema(spec);
  }

  @Test
  public void testValidatePrimaryKeys() {
    FieldValidator validator = new FieldValidator(schema);

    // Success case
    validator.validatePrimaryKeys(Arrays.asList(Fields.intField(KEY, 10), Fields.longField(KEY2, 100L),
                                                Fields.stringField(KEY3, "s")), false);

    // Success case with prefix
    validator.validatePrimaryKeys(Arrays.asList(Fields.intField(KEY, 10), Fields.longField(KEY2, 100L)),
                                                true);
    validator.validatePrimaryKeys(Collections.singletonList(Fields.intField(KEY, 10)), true);

    // Test invalid type
    try {
      validator.validatePrimaryKeys(Arrays.asList(Fields.floatField(KEY, 10.0f), Fields.longField(KEY2, 100L),
                                                  Fields.stringField(KEY3, "s")), false);
      Assert.fail("Expected InvalidFieldException");
    } catch (InvalidFieldException e) {
      // expected
    }

    // Test invalid number
    try {
      validator.validatePrimaryKeys(Arrays.asList(Fields.intField(KEY, 10), Fields.longField(KEY2, 100L)),
                                    false);
      Assert.fail("Expected InvalidFieldException");
    } catch (InvalidFieldException e) {
      // expected
    }
  }
}
