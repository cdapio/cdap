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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.http.BodyConsumer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AuthorizationDatasetTypeService} startUp and shutDown lifecycle.
 */
public class AuthorizationDatasetTypeServiceTest {

  @Test
  public void testStartAndStopDelegatesToUnderlyingDatasetTypeService() {
    final AtomicBoolean started = new AtomicBoolean(false);
    final AtomicBoolean stopped = new AtomicBoolean(false);

    DatasetTypeService delegate = new StubDatasetTypeService() {
      @Override
      protected void startUp() {
        started.set(true);
      }

      @Override
      protected void shutDown() {
        stopped.set(true);
      }
    };

    AuthorizationDatasetTypeService service = new AuthorizationDatasetTypeService(
        delegate, new NoOpAccessController(), Mockito.mock(AuthenticationContext.class));

    Assert.assertEquals(Service.State.RUNNING, service.startAndWait());
    Assert.assertTrue(started.get());
    Assert.assertEquals(Service.State.RUNNING, delegate.state());

    Assert.assertEquals(Service.State.TERMINATED, service.stopAndWait());
    Assert.assertTrue(stopped.get());
    Assert.assertEquals(Service.State.TERMINATED, delegate.state());
  }

  private abstract static class StubDatasetTypeService extends AbstractIdleService
      implements DatasetTypeService {
    @Override
    public List<DatasetModuleMeta> listModules(NamespaceId namespaceId) {
      return Collections.emptyList();
    }

    @Override
    public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) {
      return null;
    }

    @Override
    public BodyConsumer addModule(DatasetModuleId datasetModuleId, String className,
        boolean forceUpdate) {
      return null;
    }

    @Override
    public void delete(DatasetModuleId datasetModuleId) {
      // no-op
    }

    @Override
    public void deleteAll(NamespaceId namespaceId) {
      // no-op
    }

    @Override
    public List<DatasetTypeMeta> listTypes(NamespaceId namespaceId) {
      return Collections.emptyList();
    }

    @Override
    public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) {
      return null;
    }
  }
}
