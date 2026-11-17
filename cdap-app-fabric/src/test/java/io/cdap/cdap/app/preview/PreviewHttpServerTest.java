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

package io.cdap.cdap.app.preview;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonElement;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link PreviewHttpServer}.
 */
public class PreviewHttpServerTest {

  private CConfiguration createConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Preview.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.Preview.PORT, 0);
    cConf.setInt(Constants.Preview.BOSS_THREADS, 1);
    cConf.setInt(Constants.Preview.WORKER_THREADS, 1);
    cConf.setInt(Constants.Preview.EXEC_THREADS, 2);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    return cConf;
  }

  @Test
  public void testStartAndStopWithServicePreviewManager() {
    CConfiguration cConf = createConf();
    SConfiguration sConf = SConfiguration.create();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    ServicePreviewManager previewManager = new ServicePreviewManager();
    CommonNettyHttpServiceFactory factory = new CommonNettyHttpServiceFactory(
        cConf, new NoOpMetricsCollectionService(), contexts -> { }, new NoOpAeadCipher());

    PreviewHttpServer server = new PreviewHttpServer(
        cConf, sConf, discoveryService, Collections.emptySet(), previewManager, factory);
    Assert.assertSame(previewManager, server.getPreviewManager());

    server.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, server.state());
    Assert.assertEquals(Service.State.RUNNING, previewManager.state());

    ServiceDiscovered discovered = discoveryService.discover(Constants.Service.PREVIEW_HTTP);
    Assert.assertTrue(discovered.iterator().hasNext());

    server.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, server.state());
    Assert.assertEquals(Service.State.TERMINATED, previewManager.state());
    Assert.assertFalse(discovered.iterator().hasNext());
  }

  @Test
  public void testStartAndStopWithNonServicePreviewManager() {
    CConfiguration cConf = createConf();
    SConfiguration sConf = SConfiguration.create();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    PreviewManager previewManager = Mockito.mock(PreviewManager.class);
    CommonNettyHttpServiceFactory factory = new CommonNettyHttpServiceFactory(
        cConf, new NoOpMetricsCollectionService(), contexts -> { }, new NoOpAeadCipher());

    PreviewHttpServer server = new PreviewHttpServer(
        cConf, sConf, discoveryService, Collections.emptySet(), previewManager, factory);

    server.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, server.state());

    server.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, server.state());
  }

  private static final class ServicePreviewManager extends AbstractIdleService
      implements PreviewManager {

    @Override
    protected void startUp() {
      // no-op
    }

    @Override
    protected void shutDown() {
      // no-op
    }

    @Override
    public ApplicationId start(NamespaceId namespace, AppRequest<?> request) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PreviewStatus getStatus(ApplicationId applicationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stopPreview(ApplicationId applicationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<JsonElement>> getData(ApplicationId applicationId, String tracerName) {
      return Collections.emptyMap();
    }

    @Override
    public ProgramRunId getRunId(ApplicationId applicationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricsQueryHelper getMetricsQueryHelper() {
      throw new UnsupportedOperationException();
    }

    @Override
    public LogReader getLogReader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<PreviewRequest> poll(@Nullable byte[] pollerInfo) {
      return Optional.empty();
    }
  }
}
