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

package io.cdap.cdap.spi.data.common;

import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link StructuredTableRegistry}.
 */
public abstract class StructuredTableRegistryTest {
  private static final FieldType KEY_FIELD = Fields.intType("i");
  private static final FieldType STR_FIELD = Fields.stringType("s");
  private static final FieldType LONG_FIELD = Fields.longType("l");

  protected static final StructuredTableId TABLE1 = new StructuredTableId("table1");
  protected static final StructuredTableSpecification SPEC1 = new StructuredTableSpecification.Builder()
    .withId(TABLE1)
    .withFields(KEY_FIELD, STR_FIELD, LONG_FIELD)
    .withPrimaryKeys(KEY_FIELD.getName())
    .withIndexes(STR_FIELD.getName())
    .build();

  protected static final StructuredTableId TABLE2 = new StructuredTableId("table2");
  protected static final StructuredTableSpecification SPEC2 = new StructuredTableSpecification.Builder()
    .withId(TABLE2)
    .withFields(KEY_FIELD, STR_FIELD, LONG_FIELD, Fields.floatType("f"))
    .withPrimaryKeys(KEY_FIELD.getName())
    .withIndexes(LONG_FIELD.getName())
    .build();

  /**
   * @return the right implementation of the registry based on the underlying storage.
   */
  protected abstract StructuredTableRegistry getStructuredTableRegistry();

  @After
  public void cleanUp() {
    StructuredTableRegistry registry = getStructuredTableRegistry();
    if (registry.getSpecification(TABLE1) != null) {
      registry.removeSpecification(TABLE1);
    }
    if (registry.getSpecification(TABLE2) != null) {
      registry.removeSpecification(TABLE2);
    }
    Assert.assertTrue(registry.isEmpty());
  }

  @Test
  public void testRegistry() throws Exception {
    StructuredTableRegistry registry = getStructuredTableRegistry();

    // Assert empty
    Assert.assertTrue(registry.isEmpty());
    Assert.assertNull(registry.getSpecification(TABLE1));

    // Register table1
    registry.registerSpecification(SPEC1);
    Assert.assertEquals(SPEC1, registry.getSpecification(TABLE1));
    Assert.assertFalse(registry.isEmpty());

    // Register table2
    registry.registerSpecification(SPEC2);
    Assert.assertEquals(SPEC2, registry.getSpecification(TABLE2));
    Assert.assertFalse(registry.isEmpty());

    // Re-registering table1 should fail
    try {
      registry.registerSpecification(SPEC1);
      Assert.fail("Expected re-registration to fail");
    } catch (TableAlreadyExistsException e) {
      // Expected
    }

    // Remove spec for table1, and try again to register
    registry.removeSpecification(TABLE1);
    Assert.assertNull(registry.getSpecification(TABLE1));
    Assert.assertFalse(registry.isEmpty());

    // Now re-register table1
    registry.registerSpecification(SPEC1);
    Assert.assertEquals(SPEC1, registry.getSpecification(TABLE1));

    // Delete both the specs
    registry.removeSpecification(TABLE1);
    registry.removeSpecification(TABLE2);
    Assert.assertTrue(registry.isEmpty());
  }
}
