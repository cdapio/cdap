/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * This is a test base for {@link StructuredTableAdmin}.
 */
public abstract class StructuredTableAdminTest {
  private static final FieldType KEY_FIELD = Fields.intType("i");
  private static final FieldType STR_FIELD = Fields.stringType("s");
  private static final FieldType LONG_FIELD = Fields.longType("l");
  private static final FieldType BOOL_FIELD = Fields.booleanType("b");
  private static final FieldType IDX_FIELD = Fields.stringType("idx");
  protected static final StructuredTableId SIMPLE_TABLE = new StructuredTableId("simple_table");
  protected static final StructuredTableId INCONSISTENT_PRIMARY_KEY_TABLE =
    new StructuredTableId("another_table");

  protected static final StructuredTableSpecification SIMPLE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(SIMPLE_TABLE)
      .withFields(KEY_FIELD, STR_FIELD, LONG_FIELD, BOOL_FIELD)
      .withPrimaryKeys(KEY_FIELD.getName())
      .withIndexes(BOOL_FIELD.getName())
      .build();

  protected static final StructuredTableSpecification UPDATED_SIMPLE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(SIMPLE_TABLE)
      .withFields(
        KEY_FIELD,
        STR_FIELD,
        LONG_FIELD,
        BOOL_FIELD,
        Fields.floatType("updated_field1"),
        Fields.longType("updated_field2"))
      .withPrimaryKeys(KEY_FIELD.getName())
      .withIndexes(STR_FIELD.getName(), LONG_FIELD.getName(), BOOL_FIELD.getName())
      .build();

  protected static final StructuredTableSpecification INCOMPATIBLE_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(SIMPLE_TABLE)
      .withFields(KEY_FIELD, STR_FIELD, Fields.floatType("updated_field1"))
      .withPrimaryKeys(KEY_FIELD.getName())
      .build();

  protected static final StructuredTableSpecification INCONSISTENT_PRIMARY_KEY_TABLE_SPEC =
    new StructuredTableSpecification.Builder()
      .withId(INCONSISTENT_PRIMARY_KEY_TABLE)
      .withFields(KEY_FIELD, STR_FIELD, LONG_FIELD, IDX_FIELD, BOOL_FIELD)
      .withPrimaryKeys(KEY_FIELD.getName(), LONG_FIELD.getName(), STR_FIELD.getName())
      .withIndexes(IDX_FIELD.getName(), BOOL_FIELD.getName())
      .build();

  /**
   * @return the right implementation of the table admin based on the underlying storage.
   */
  protected abstract StructuredTableAdmin getStructuredTableAdmin() throws Exception;


  @After
  public void cleanUp() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();
    if (admin.exists(SIMPLE_TABLE)) {
      admin.drop(SIMPLE_TABLE);
    }
    if (admin.exists(INCONSISTENT_PRIMARY_KEY_TABLE)) {
      admin.drop(INCONSISTENT_PRIMARY_KEY_TABLE);
    }
    Assert.assertFalse(admin.exists(SIMPLE_TABLE));
    Assert.assertFalse(admin.exists(INCONSISTENT_PRIMARY_KEY_TABLE));
  }

  @Test
  public void testAdmin() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert SIMPLE_TABLE Empty
    Assert.assertFalse(admin.exists(SIMPLE_TABLE));

    // getSchema SIMPLE_TABLE should fail
    try {
      admin.getSchema(SIMPLE_TABLE);
      Assert.fail("Expected getSchema SIMPLE_TABLE to fail");
    } catch (TableNotFoundException e) {
      // Expected
    }

    // Create SIMPLE_TABLE
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    Assert.assertTrue(admin.exists(SIMPLE_TABLE));

    // Assert SIMPLE_TABLE schema
    StructuredTableSchema simpleTableSchema = admin.getSchema(SIMPLE_TABLE);
    Assert.assertEquals(simpleTableSchema, new StructuredTableSchema(SIMPLE_TABLE_SPEC));

    // Update SIMPLE_TABLE spec to UPDATED_SIMPLE_TABLE_SPEC
    admin.createOrUpdate(UPDATED_SIMPLE_TABLE_SPEC);

    // Assert UPDATED_SIMPLE_TABLE_SPEC schema
    StructuredTableSchema updateSimpleTableSchema = admin.getSchema(SIMPLE_TABLE);
    StructuredTableSchema expected = new StructuredTableSchema(UPDATED_SIMPLE_TABLE_SPEC);
    Assert.assertEquals(updateSimpleTableSchema, expected);
  }

  @Test
  public void testCreateOrUpdateTwiceShouldSucceed() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert SIMPLE_TABLE Empty
    Assert.assertFalse(admin.exists(SIMPLE_TABLE));

    // Calling to createOrUpdate the same SIMPLE_TABLE spec twice to mimic the scenario of
    // connecting to an exsting DB
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    Assert.assertTrue(admin.exists(SIMPLE_TABLE));

    // Assert SIMPLE_TABLE schema
    StructuredTableSchema simpleTableSchema = admin.getSchema(SIMPLE_TABLE);
    Assert.assertEquals(simpleTableSchema, new StructuredTableSchema(SIMPLE_TABLE_SPEC));
  }

  @Test
  public void testBackwardCompatible() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert SIMPLE_TABLE Empty
    Assert.assertFalse(admin.exists(SIMPLE_TABLE));

    // Create SIMPLE_TABLE
    admin.createOrUpdate(SIMPLE_TABLE_SPEC);
    Assert.assertTrue(admin.exists(SIMPLE_TABLE));

    // getSchema SIMPLE_TABLE should fail
    try {
      admin.createOrUpdate(INCOMPATIBLE_TABLE_SPEC);
      Assert.fail("Expected createOrUpdate INCOMPATIBLE_TABLE_SPEC to fail");
    } catch (TableSchemaIncompatibleException e) {
      // Expected
    }
  }

  @Test
  public void testInconsistentKeyOrderInSchema() throws Exception {
    StructuredTableAdmin admin = getStructuredTableAdmin();

    // Assert INCONSISTENT_PRIMARY_KEY_TABLE Empty
    Assert.assertFalse(admin.exists(INCONSISTENT_PRIMARY_KEY_TABLE));

    // Create INCONSISTENT_PRIMARY_KEY_TABLE
    admin.createOrUpdate(INCONSISTENT_PRIMARY_KEY_TABLE_SPEC);
    Assert.assertTrue(admin.exists(INCONSISTENT_PRIMARY_KEY_TABLE));

    // Assert INCONSISTENT_PRIMARY_KEY_TABLE schema
    StructuredTableSchema tableSchema = admin.getSchema(INCONSISTENT_PRIMARY_KEY_TABLE);
    Assert.assertEquals(tableSchema, new StructuredTableSchema(INCONSISTENT_PRIMARY_KEY_TABLE_SPEC));
  }
}
