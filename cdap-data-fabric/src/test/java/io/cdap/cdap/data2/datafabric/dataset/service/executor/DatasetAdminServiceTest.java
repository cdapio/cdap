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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import io.cdap.cdap.data2.datafabric.dataset.DatasetType;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DatasetAdminService} ensuring {@link DatasetAdmin} is closed quietly.
 */
public class DatasetAdminServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCreateDropAndPerformAdminCloseAdminQuietlyEvenWhenCloseThrows() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Impersonator impersonator = new DefaultImpersonator(cConf, null);

    final AtomicInteger closeCalls = new AtomicInteger(0);
    final AtomicInteger createCalls = new AtomicInteger(0);
    final AtomicInteger dropCalls = new AtomicInteger(0);
    final AtomicInteger truncateCalls = new AtomicInteger(0);

    DatasetAdmin throwingCloseAdmin = new DatasetAdmin() {
      @Override
      public boolean exists() {
        return true;
      }

      @Override
      public void create() {
        createCalls.incrementAndGet();
      }

      @Override
      public void drop() {
        dropCalls.incrementAndGet();
      }

      @Override
      public void truncate() {
        truncateCalls.incrementAndGet();
      }

      @Override
      public void upgrade() {
        // no-op
      }

      @Override
      public void close() throws IOException {
        closeCalls.incrementAndGet();
        throw new IOException("simulated DatasetAdmin close failure");
      }
    };

    Dataset dummyDataset = () -> {
      // no-op close
    };

    DatasetDefinition<Dataset, DatasetAdmin> definition =
        new DatasetDefinition<Dataset, DatasetAdmin>() {
          @Override
          public String getName() {
            return "testType";
          }

          @Override
          public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
            return DatasetSpecification.builder(instanceName, getName())
                .properties(properties.getProperties())
                .build();
          }

          @Override
          public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
              ClassLoader classLoader) {
            return throwingCloseAdmin;
          }

          @Override
          public Dataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
              Map<String, String> arguments, ClassLoader classLoader) {
            return dummyDataset;
          }
        };

    DatasetType datasetType = new DatasetType(definition, getClass().getClassLoader());
    RemoteDatasetFramework remoteFramework = Mockito.mock(RemoteDatasetFramework.class);
    Mockito.when(remoteFramework.getDatasetType(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(datasetType);

    DatasetId datasetId = NamespaceId.DEFAULT.dataset("myDs");
    SystemDatasetInstantiator instantiator = Mockito.mock(SystemDatasetInstantiator.class);
    Mockito.when(instantiator.getDatasetAdmin(datasetId)).thenReturn(throwingCloseAdmin);
    SystemDatasetInstantiatorFactory instantiatorFactory =
        Mockito.mock(SystemDatasetInstantiatorFactory.class);
    Mockito.when(instantiatorFactory.create()).thenReturn(instantiator);

    DatasetAdminService adminService = new DatasetAdminService(
        remoteFramework, cConf, locationFactory, instantiatorFactory, impersonator);

    DatasetTypeMeta typeMeta = new DatasetTypeMeta("testType", Collections.emptyList());

    // 1. createOrUpdate closes admin quietly despite IOException from admin.close()
    DatasetCreationResponse response =
        adminService.createOrUpdate(datasetId, typeMeta, DatasetProperties.EMPTY, null);
    Assert.assertNotNull(response.getSpec());
    Assert.assertEquals(1, createCalls.get());
    Assert.assertEquals(1, closeCalls.get());

    // 2. exists / truncate (performDatasetAdmin) closes ImpersonatingDatasetAdmin quietly
    Assert.assertTrue(adminService.exists(datasetId));
    Assert.assertEquals(2, closeCalls.get());
    adminService.truncate(datasetId);
    Assert.assertEquals(1, truncateCalls.get());
    Assert.assertEquals(3, closeCalls.get());

    // 3. drop closes admin quietly despite IOException from admin.close()
    adminService.drop(datasetId, typeMeta, response.getSpec());
    Assert.assertEquals(1, dropCalls.get());
    Assert.assertEquals(4, closeCalls.get());
  }
}
