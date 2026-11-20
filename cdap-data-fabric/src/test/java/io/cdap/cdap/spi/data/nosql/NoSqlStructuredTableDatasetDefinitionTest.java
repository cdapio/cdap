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

package io.cdap.cdap.spi.data.nosql;

import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link NoSqlStructuredTableDatasetDefinition}.
 */
public class NoSqlStructuredTableDatasetDefinitionTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testDelegateAndGetDatasetExceptionPropagation() throws Exception {
    DatasetDefinition delegate = Mockito.mock(DatasetDefinition.class);
    Dataset mockDataset = Mockito.mock(Dataset.class);
    DatasetAdmin mockAdmin = Mockito.mock(DatasetAdmin.class);
    DatasetContext context = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());
    DatasetSpecification spec = DatasetSpecification.builder("sysTable", "table").build();

    Mockito.when(delegate.getName()).thenReturn("table");
    Mockito.when(delegate.configure("sysTable", DatasetProperties.EMPTY)).thenReturn(spec);
    Mockito.when(delegate.getAdmin(context, spec, null)).thenReturn(mockAdmin);

    final IOException expectedIo = new IOException("io failure");
    final IllegalArgumentException expectedUnchecked = new IllegalArgumentException("bad arg");
    final Exception expectedChecked = new Exception("checked failure");

    Mockito.when(delegate.getDataset(context, spec, Collections.emptyMap(), null))
        .thenReturn(mockDataset)
        .thenThrow(expectedIo)
        .thenThrow(expectedUnchecked)
        .thenAnswer(inv -> {
          throw expectedChecked;
        });

    NoSqlStructuredTableDatasetDefinition wrapper =
        new NoSqlStructuredTableDatasetDefinition(delegate);
    Assert.assertEquals("table", wrapper.getName());
    Assert.assertSame(spec, wrapper.configure("sysTable", DatasetProperties.EMPTY));
    Assert.assertSame(mockAdmin, wrapper.getAdmin(context, spec, null));
    Assert.assertSame(mockDataset, wrapper.getDataset(context, spec, Collections.emptyMap(), null));

    try {
      wrapper.getDataset(context, spec, Collections.emptyMap(), null);
      Assert.fail("Expected IOException to propagate directly");
    } catch (IOException actual) {
      Assert.assertSame(expectedIo, actual);
    }

    try {
      wrapper.getDataset(context, spec, Collections.emptyMap(), null);
      Assert.fail("Expected unchecked RuntimeException to propagate directly");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedUnchecked, actual);
    }

    try {
      wrapper.getDataset(context, spec, Collections.emptyMap(), null);
      Assert.fail("Expected checked exception wrapped in RuntimeException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedChecked, actual.getCause());
    }
  }
}
