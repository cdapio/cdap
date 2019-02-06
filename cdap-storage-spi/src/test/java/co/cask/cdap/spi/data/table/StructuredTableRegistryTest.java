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

package co.cask.cdap.spi.data.table;

import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.Fields;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public abstract class StructuredTableRegistryTest {

  protected abstract StructuredTableRegistry getStructuredTableSpecificationRegistry();

  @Test
  public void testRegistry() throws Exception {
    StructuredTableRegistry registry = getStructuredTableSpecificationRegistry();
    registry.initialize();

    StructuredTableId tableId1 = new StructuredTableId("table1");
    FieldType key = Fields.intType("i");
    FieldType str = Fields.stringType("s");
    FieldType lng = Fields.longType("l");
    StructuredTableSpecification spec1 = new StructuredTableSpecification.Builder()
      .withId(tableId1)
      .withFields(key, str, lng)
      .withPrimaryKeys(key.getName())
      .withIndexes(str.getName())
      .build();

    // Assert empty
    Assert.assertTrue(registry.isEmpty());
    Assert.assertNull(registry.getSpecification(tableId1));

    // Register table1
    registry.registerSpecification(spec1);
    Assert.assertEquals(spec1, registry.getSpecification(tableId1));
    Assert.assertFalse(registry.isEmpty());

    // Register table2
    StructuredTableId tableId2 = new StructuredTableId("table2");
    StructuredTableSpecification spec2 = new StructuredTableSpecification.Builder()
      .withId(tableId2)
      .withFields(key, str, lng, Fields.floatType("f"))
      .withPrimaryKeys(key.getName())
      .withIndexes(lng.getName())
      .build();

    registry.registerSpecification(spec2);
    Assert.assertEquals(spec2, registry.getSpecification(tableId2));
    Assert.assertFalse(registry.isEmpty());

    // Re-registering table1 should fail
    try {
      registry.registerSpecification(spec1);
      Assert.fail("Expected re-registration to fail");
    } catch (TableAlreadyExistsException e) {
      // Expected
    }

    // Remove spec for table1, and try again to register
    registry.removeSpecification(tableId1);
    Assert.assertNull(registry.getSpecification(tableId1));
    Assert.assertFalse(registry.isEmpty());

    // Now re-register table1
    registry.registerSpecification(spec1);
    Assert.assertEquals(spec1, registry.getSpecification(tableId1));

    // Delete both the specs
    registry.removeSpecification(tableId1);
    registry.removeSpecification(tableId2);
    Assert.assertTrue(registry.isEmpty());
  }
}
