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

package io.cdap.cdap.data2.datafabric.dataset;

import com.google.inject.Guice;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.data2.datafabric.dataset.service.TestModule1;
import io.cdap.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Direct unit tests for {@link RemoteDatasetFramework#getDatasetType} Guava paths.
 */
public class RemoteDatasetFrameworkGetTypeTest {

  private static final IllegalStateException MODULE_RUNTIME_FAILURE =
      new IllegalStateException("module registration unchecked failure");

  /**
   * Test module that throws an unchecked {@link RuntimeException} during registration.
   */
  public static class ThrowingDatasetModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      throw MODULE_RUNTIME_FAILURE;
    }
  }

  @Test
  public void testGetDatasetTypeResolvesNullClassLoaderEvenWhenContextClassLoaderIsNull() {
    RemoteDatasetFramework framework = createFramework();
    DatasetModuleMeta moduleMeta = new DatasetModuleMeta(
        "module1", TestModule1.class.getName(), null,
        Collections.singletonList("datasetType1"), Collections.emptyList());
    DatasetTypeMeta typeMeta =
        new DatasetTypeMeta("datasetType1", Collections.singletonList(moduleMeta));

    ClassLoader previousContextCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(null);
      DatasetType type = framework.getDatasetType(
          typeMeta, null, new ConstantClassLoaderProvider(TestModule1.class.getClassLoader()));
      Assert.assertNotNull(type);
    } finally {
      Thread.currentThread().setContextClassLoader(previousContextCl);
    }
  }

  @Test
  public void testGetDatasetTypePropagatesClassLoaderIoExceptionAndCheckedAndUncheckedModuleErrors()
      throws Exception {
    RemoteDatasetFramework framework = createFramework();
    DatasetModuleMeta validModuleMeta = new DatasetModuleMeta(
        "module1", TestModule1.class.getName(), null,
        Collections.singletonList("datasetType1"), Collections.emptyList());
    DatasetTypeMeta validTypeMeta =
        new DatasetTypeMeta("datasetType1", Collections.singletonList(validModuleMeta));

    // 1. IOException from classLoaderProvider.get() wraps in RuntimeException
    final IOException expectedIo = new IOException("jar unpack failure");
    DatasetClassLoaderProvider failingClProvider = Mockito.mock(DatasetClassLoaderProvider.class);
    Mockito.when(failingClProvider.get(Mockito.any(), Mockito.any())).thenThrow(expectedIo);
    try {
      framework.getDatasetType(validTypeMeta, getClass().getClassLoader(), failingClProvider);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedIo, actual.getCause());
    }

    // 2. Checked ClassNotFoundException wraps in RuntimeException
    DatasetModuleMeta missingClassMeta = new DatasetModuleMeta(
        "missing", "io.cdap.cdap.NonExistentDatasetModule", null,
        Collections.singletonList("t"), Collections.emptyList());
    DatasetTypeMeta missingTypeMeta =
        new DatasetTypeMeta("t", Collections.singletonList(missingClassMeta));
    try {
      framework.getDatasetType(
          missingTypeMeta, getClass().getClassLoader(), new ConstantClassLoaderProvider());
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException actual) {
      Assert.assertTrue(actual.getCause() instanceof ClassNotFoundException);
    }

    // 3. Unchecked RuntimeException from module registration must rethrow exact instance (no double-wrap!)
    DatasetModuleMeta throwingModuleMeta = new DatasetModuleMeta(
        "throwing", ThrowingDatasetModule.class.getName(), null,
        Collections.singletonList("t"), Collections.emptyList());
    DatasetTypeMeta throwingTypeMeta =
        new DatasetTypeMeta("t", Collections.singletonList(throwingModuleMeta));
    try {
      framework.getDatasetType(
          throwingTypeMeta, getClass().getClassLoader(), new ConstantClassLoaderProvider());
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(MODULE_RUNTIME_FAILURE, actual);
    }
  }

  private static RemoteDatasetFramework createFramework() {
    return new RemoteDatasetFramework(
        CConfiguration.create(),
        new DefaultDatasetDefinitionRegistryFactory(Guice.createInjector()),
        Mockito.mock(AuthenticationContext.class),
        Mockito.mock(RemoteClientFactory.class));
  }
}
