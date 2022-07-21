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

package io.cdap.cdap.spi.data.common;

import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for {@link CachedStructuredTableRegistry}.
 */
public abstract class CachedStructuredTableRegistryTest extends StructuredTableRegistryTest {
  /**
   * @return non-cached version of StructuredTableRegistry
   */
  protected abstract StructuredTableRegistry getNonCachedStructuredTableRegistry();

  @Test
  public void testCachedRegistryRead() throws IOException, TableAlreadyExistsException {
    // Get cached registry
    StructuredTableRegistry cachedRegistry = getStructuredTableRegistry();
    // Assert empty
    Assert.assertTrue(cachedRegistry.isEmpty());
    Assert.assertNull(cachedRegistry.getSpecification(TABLE1));

    // Get non-cached registry
    StructuredTableRegistry nonCachedRegistry = getNonCachedStructuredTableRegistry();
    // Assert empty
    Assert.assertTrue(nonCachedRegistry.isEmpty());
    Assert.assertNull(nonCachedRegistry.getSpecification(TABLE1));

    // Register table1 using non-cached registry
    nonCachedRegistry.registerSpecification(SPEC1);
    Assert.assertEquals(SPEC1, nonCachedRegistry.getSpecification(TABLE1));
    Assert.assertFalse(cachedRegistry.isEmpty());

    // Should be able to read the spec using the cached registry too
    Assert.assertEquals(SPEC1, cachedRegistry.getSpecification(TABLE1));
    Assert.assertFalse(cachedRegistry.isEmpty());

    // Remove the spec for table1 using the non-cached registry
    nonCachedRegistry.removeSpecification(TABLE1);
    Assert.assertNull(nonCachedRegistry.getSpecification(TABLE1));
    // However, the cached registry should still return SPEC1
    Assert.assertEquals(SPEC1, cachedRegistry.getSpecification(TABLE1));
  }

  @Test
  public void testCachedRegistryWrite() throws IOException, TableAlreadyExistsException {
    // Get cached registry
    StructuredTableRegistry cachedRegistry = getStructuredTableRegistry();
    // Assert empty
    Assert.assertTrue(cachedRegistry.isEmpty());
    Assert.assertNull(cachedRegistry.getSpecification(TABLE1));

    // Get non-cached registry
    StructuredTableRegistry nonCachedRegistry = getNonCachedStructuredTableRegistry();
    // Assert empty
    Assert.assertTrue(nonCachedRegistry.isEmpty());
    Assert.assertNull(nonCachedRegistry.getSpecification(TABLE1));

    // Register table1 using cached registry
    cachedRegistry.registerSpecification(SPEC1);
    // Should be visible in both registries
    Assert.assertEquals(SPEC1, cachedRegistry.getSpecification(TABLE1));
    Assert.assertEquals(SPEC1, nonCachedRegistry.getSpecification(TABLE1));

    // Remove table1 spec using cached registry
    cachedRegistry.removeSpecification(TABLE1);
    // Removal should be visible in both the registries
    Assert.assertNull(cachedRegistry.getSpecification(TABLE1));
    Assert.assertNull(nonCachedRegistry.getSpecification(TABLE1));
  }
}
