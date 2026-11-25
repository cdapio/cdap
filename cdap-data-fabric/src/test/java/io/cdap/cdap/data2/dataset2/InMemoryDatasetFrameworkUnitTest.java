/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2;

import com.google.inject.Guice;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct unit tests for {@link InMemoryDatasetFramework#createRegistry} exception propagation.
 */
public class InMemoryDatasetFrameworkUnitTest {

  private static final IllegalStateException MODULE_UNCHECKED_ERROR =
      new IllegalStateException("unchecked module registration error");

  /**
   * DatasetModule that throws an unchecked exception on registration.
   */
  public static class UncheckedThrowingModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      throw MODULE_UNCHECKED_ERROR;
    }
  }

  @Test
  public void testCreateRegistryPropagatesUncheckedDirectlyAndWrapsCheckedException() {
    InMemoryDatasetFramework framework = new InMemoryDatasetFramework(
        new DefaultDatasetDefinitionRegistryFactory(Guice.createInjector()));

    // 1. Checked ClassNotFoundException wraps in RuntimeException
    LinkedHashSet<String> missingSet =
        new LinkedHashSet<>(Collections.singletonList("io.cdap.cdap.NonExistentModule"));
    try {
      framework.createRegistry(missingSet, getClass().getClassLoader());
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException actual) {
      Assert.assertTrue(actual.getCause() instanceof ClassNotFoundException);
    }

    // 2. Unchecked RuntimeException rethrows exact instance without double-wrapping
    LinkedHashSet<String> throwingSet =
        new LinkedHashSet<>(Collections.singletonList(UncheckedThrowingModule.class.getName()));
    try {
      framework.createRegistry(throwingSet, getClass().getClassLoader());
      Assert.fail("Expected exact MODULE_UNCHECKED_ERROR instance");
    } catch (RuntimeException actual) {
      Assert.assertSame(MODULE_UNCHECKED_ERROR, actual);
    }
  }
}
