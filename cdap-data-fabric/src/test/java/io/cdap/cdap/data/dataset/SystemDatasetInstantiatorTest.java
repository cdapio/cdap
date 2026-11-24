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

package io.cdap.cdap.data.dataset;

import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SystemDatasetInstantiator}.
 */
public class SystemDatasetInstantiatorTest {

  @Test
  public void testParentClassLoaderFallbackWhenContextClassLoaderIsNullAndNonNull() throws Exception {
    final DatasetId datasetId = NamespaceId.DEFAULT.dataset("ds1");
    Dataset mockDataset = Mockito.mock(Dataset.class);
    DatasetClassLoaderProvider clProvider = Mockito.mock(DatasetClassLoaderProvider.class);

    ClassLoader originalContextCl = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader customContextCl = new URLClassLoader(new URL[0], getClass().getClassLoader())) {
      // Case 1: null parentClassLoader + non-null contextClassLoader -> uses contextClassLoader
      Thread.currentThread().setContextClassLoader(customContextCl);
      DatasetFramework fw1 = Mockito.mock(DatasetFramework.class);
      ArgumentCaptor<ClassLoader> clCaptor1 = ArgumentCaptor.forClass(ClassLoader.class);
      Mockito.when(fw1.getDataset(Mockito.eq(datasetId), Mockito.anyMap(),
          clCaptor1.capture(), Mockito.eq(clProvider), Mockito.any(), Mockito.eq(AccessType.UNKNOWN)))
          .thenReturn(mockDataset);

      SystemDatasetInstantiator instantiator1 =
          new SystemDatasetInstantiator(fw1, null, clProvider, null);
      Assert.assertSame(mockDataset, instantiator1.getDataset(datasetId));
      Assert.assertSame(customContextCl, clCaptor1.getValue());

      // Case 2: null parentClassLoader + null contextClassLoader -> falls back to getClass().getClassLoader()
      Thread.currentThread().setContextClassLoader(null);
      DatasetFramework fw2 = Mockito.mock(DatasetFramework.class);
      ArgumentCaptor<ClassLoader> clCaptor2 = ArgumentCaptor.forClass(ClassLoader.class);
      Mockito.when(fw2.getDataset(Mockito.eq(datasetId), Mockito.anyMap(),
          clCaptor2.capture(), Mockito.eq(clProvider), Mockito.any(), Mockito.eq(AccessType.UNKNOWN)))
          .thenReturn(mockDataset);

      SystemDatasetInstantiator instantiator2 =
          new SystemDatasetInstantiator(fw2, null, clProvider, null);
      Assert.assertSame(mockDataset, instantiator2.getDataset(datasetId));
      Assert.assertSame(SystemDatasetInstantiator.class.getClassLoader(), clCaptor2.getValue());
      instantiator2.close();
      Mockito.verify(clProvider).close();
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextCl);
    }
  }

  @Test
  public void testGetDatasetPropagatesServiceUnavailableExceptionDirectly() throws Exception {
    final DatasetId datasetId = NamespaceId.DEFAULT.dataset("unavailableDs");
    final ServiceUnavailableException expectedUnavailable =
        new ServiceUnavailableException("dataset.service");

    DatasetFramework fw = Mockito.mock(DatasetFramework.class);
    Mockito.when(fw.getDataset(Mockito.eq(datasetId), Mockito.anyMap(),
        Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenThrow(expectedUnavailable);

    try (SystemDatasetInstantiator instantiator = new SystemDatasetInstantiator(fw)) {
      try {
        instantiator.getDataset(datasetId, Collections.emptyMap(), AccessType.READ);
        Assert.fail("Expected ServiceUnavailableException");
      } catch (ServiceUnavailableException actual) {
        Assert.assertSame(expectedUnavailable, actual);
      }
    }
  }

  @Test
  public void testGetDatasetWrapsOtherExceptionsAndNullDatasetInDatasetInstantiationException()
      throws Exception {
    final DatasetId datasetId = NamespaceId.DEFAULT.dataset("brokenDs");
    final DatasetManagementException expectedManagementEx =
        new DatasetManagementException("management error");

    DatasetFramework fw = Mockito.mock(DatasetFramework.class);
    Mockito.when(fw.getDataset(Mockito.eq(datasetId), Mockito.anyMap(),
        Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenThrow(expectedManagementEx)
        .thenReturn(null);

    try (SystemDatasetInstantiator instantiator = new SystemDatasetInstantiator(fw)) {
      try {
        instantiator.getDataset(datasetId);
        Assert.fail("Expected DatasetInstantiationException wrapping DatasetManagementException");
      } catch (DatasetInstantiationException actual) {
        Assert.assertSame(expectedManagementEx, actual.getCause());
      }

      try {
        instantiator.getDataset(datasetId);
        Assert.fail("Expected DatasetInstantiationException when dataset does not exist");
      } catch (DatasetInstantiationException actual) {
        Assert.assertTrue(actual.getMessage().contains(datasetId.toString()));
      }
    }
  }
}
