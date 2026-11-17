/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fast unit tests for {@link TaskWorkerService} start and automatic stop callback.
 */
public class TaskWorkerServiceUnitTest {

  private static final Gson GSON = new Gson();

  /**
   * Fast no-sleep runnable task for exercising {@code stopService} after request completion.
   */
  public static final class EchoTask implements RunnableTask {
    @Override
    public void run(RunnableTaskContext context) throws Exception {
      context.writeResult(context.getParam().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testRequestCompletionTriggersServiceStop() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.set(Constants.ArtifactLocalizer.PRELOAD_LIST, "");
    cConf.setInt(Constants.ArtifactLocalizer.PRELOAD_VERSION_LIMIT, 1);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_REQUEST_COUNT, 1);
    cConf.setInt(Constants.TaskWorker.CONTAINER_KILL_AFTER_DURATION_SECOND, 0);

    MetricsCollectionService metrics = new NoOpMetricsCollectionService();
    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
    TaskWorkerService service = new TaskWorkerService(
        cConf,
        SConfiguration.create(),
        discovery,
        discovery,
        metrics,
        new CommonNettyHttpServiceFactory(cConf, metrics, contexts -> { }, new NoOpAeadCipher()));

    CompletableFuture<Service.State> completion = TaskWorkerTestUtil.getServiceCompletionFuture(service);
    service.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, service.state());

    InetSocketAddress addr = service.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));
    RunnableTaskRequest req = RunnableTaskRequest.getBuilder(EchoTask.class.getName())
        .withParam("fast-ok")
        .withNamespace("default")
        .build();

    HttpResponse response = HttpRequests.execute(
        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
            .withBody(GSON.toJson(req))
            .build(),
        new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals("fast-ok", response.getResponseBodyAsString());
    completion.get(5, TimeUnit.SECONDS);
    Assert.assertEquals(Service.State.TERMINATED, service.state());
  }
}
