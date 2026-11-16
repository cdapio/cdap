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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data2.metrics.DatasetMetricsReporter;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.BodyConsumer;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Direct unit tests for {@link DatasetService} lifecycle, metrics reporters, discovery watch, and toString.
 */
public class DatasetServiceTest {

  @Test
  public void testLifecycleMetricsReporterDiscoveryWatchAndToString() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.Dataset.Manager.PORT, 0);

    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
    // Pre-register DATASET_EXECUTOR so DatasetService.watchChanges fires immediately via sameThreadExecutor
    discovery.register(new Discoverable(
        Constants.Service.DATASET_EXECUTOR, new InetSocketAddress("127.0.0.1", 12345)));

    final CountDownLatch reporterStarted = new CountDownLatch(1);
    final CountDownLatch reporterStopped = new CountDownLatch(1);
    DatasetMetricsReporter metricsReporter = new TestMetricsReporter(reporterStarted, reporterStopped);

    DatasetTypeService typeService = new NoOpDatasetTypeService();
    DatasetInstanceService instanceService = Mockito.mock(DatasetInstanceService.class);
    CommonNettyHttpServiceFactory nettyFactory = new CommonNettyHttpServiceFactory(
        cConf, new NoOpMetricsCollectionService(), entry -> { }, null);

    DatasetService service = new DatasetService(
        cConf,
        SConfiguration.create(),
        discovery,
        discovery,
        nettyFactory,
        Collections.singleton(metricsReporter),
        typeService,
        instanceService);

    Assert.assertEquals(Service.State.RUNNING, service.startAndWait());
    Assert.assertEquals(Service.State.RUNNING, typeService.state());
    Assert.assertTrue(reporterStarted.await(5, TimeUnit.SECONDS));

    String str = service.toString();
    Assert.assertTrue("Unexpected toString: " + str, str.startsWith("DatasetService{bindAddress="));

    Assert.assertEquals(Service.State.TERMINATED, service.stopAndWait());
    Assert.assertEquals(Service.State.TERMINATED, typeService.state());
    Assert.assertTrue(reporterStopped.await(5, TimeUnit.SECONDS));
  }

  private static final class TestMetricsReporter extends AbstractIdleService
      implements DatasetMetricsReporter {
    private final CountDownLatch started;
    private final CountDownLatch stopped;

    TestMetricsReporter(CountDownLatch started, CountDownLatch stopped) {
      this.started = started;
      this.stopped = stopped;
    }

    @Override
    protected void startUp() {
      started.countDown();
    }

    @Override
    protected void shutDown() {
      stopped.countDown();
    }
  }

  private static final class NoOpDatasetTypeService extends AbstractIdleService
      implements DatasetTypeService {
    @Override
    protected void startUp() {
      // no-op
    }

    @Override
    protected void shutDown() {
      // no-op
    }

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
