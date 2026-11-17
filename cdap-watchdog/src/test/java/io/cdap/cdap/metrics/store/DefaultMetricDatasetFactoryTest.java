/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.metrics.store;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.data2.dataset2.lib.timeseries.FactTable;
import io.cdap.cdap.metrics.process.MetricsConsumerMetaTable;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultMetricDatasetFactory} table caching and exception propagation.
 */
public class DefaultMetricDatasetFactoryTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateConsumerMetaAndFactTableCachesExistingDatasets() throws Exception {
    DatasetDefinition<MetricsTable, DatasetAdmin> tableDef = Mockito.mock(DatasetDefinition.class);
    DatasetAdmin admin = Mockito.mock(DatasetAdmin.class);
    MetricsTable metricsTable = Mockito.mock(MetricsTable.class);

    Mockito.when(tableDef.configure(Mockito.anyString(), Mockito.any(DatasetProperties.class)))
        .thenAnswer(inv -> DatasetSpecification.builder(
            inv.getArgument(0), "metricsTable").build());
    Mockito.when(tableDef.getAdmin(
        Mockito.any(DatasetContext.class),
        Mockito.any(DatasetSpecification.class),
        Mockito.any(ClassLoader.class))).thenReturn(admin);
    Mockito.when(admin.exists()).thenReturn(false);
    Mockito.when(tableDef.getDataset(
        Mockito.any(DatasetContext.class),
        Mockito.any(DatasetSpecification.class),
        Mockito.anyMap(),
        Mockito.any(ClassLoader.class))).thenReturn(metricsTable);

    DefaultMetricDatasetFactory factory =
        new DefaultMetricDatasetFactory(CConfiguration.create(), tableDef);

    MetricsConsumerMetaTable meta1 = factory.createConsumerMeta();
    MetricsConsumerMetaTable meta2 = factory.createConsumerMeta();
    Assert.assertNotNull(meta1);
    Assert.assertNotNull(meta2);
    // Second call uses existingDatasets cache so admin.create() is invoked once for meta table
    Mockito.verify(admin, Mockito.times(1)).create();

    FactTable factTable = factory.getOrCreateFactTable(1);
    Assert.assertNotNull(factTable);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCheckedIoExceptionWrappedInRuntimeException() throws Exception {
    final IOException createTableError = new IOException("dataset admin create failed");
    DatasetDefinition<MetricsTable, DatasetAdmin> tableDef = Mockito.mock(DatasetDefinition.class);
    DatasetAdmin admin = Mockito.mock(DatasetAdmin.class);

    Mockito.when(tableDef.configure(Mockito.anyString(), Mockito.any(DatasetProperties.class)))
        .thenReturn(DatasetSpecification.builder("meta", "metricsTable").build());
    Mockito.when(tableDef.getAdmin(
        Mockito.any(DatasetContext.class),
        Mockito.any(DatasetSpecification.class),
        Mockito.any(ClassLoader.class))).thenReturn(admin);
    Mockito.when(admin.exists()).thenReturn(false);
    Mockito.doThrow(createTableError).when(admin).create();

    DefaultMetricDatasetFactory factory =
        new DefaultMetricDatasetFactory(CConfiguration.create(), tableDef);

    try {
      factory.createConsumerMeta();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(createTableError, actual.getCause());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUncheckedRuntimeExceptionIsNotDoubleWrapped() throws Exception {
    final IllegalStateException uncheckedFailure =
        new IllegalStateException("dataset definition broken");
    DatasetDefinition<MetricsTable, DatasetAdmin> tableDef = Mockito.mock(DatasetDefinition.class);
    Mockito.when(tableDef.configure(Mockito.anyString(), Mockito.any(DatasetProperties.class)))
        .thenThrow(uncheckedFailure);

    DefaultMetricDatasetFactory factory =
        new DefaultMetricDatasetFactory(CConfiguration.create(), tableDef);

    try {
      factory.getOrCreateFactTable(60);
      Assert.fail("Expected IllegalStateException without double-wrapping");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedFailure, actual);
    }
  }
}
