/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import static org.mockito.Mockito.mock;

import com.sun.net.httpserver.HttpServer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.ServiceController;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KubeTwillControllerTest {

  private HttpServer server;
  private ApiClient apiClient;
  private List<String> requests;

  @Before
  public void setUp() throws IOException {
    requests = new CopyOnWriteArrayList<>();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      String query = exchange.getRequestURI().getRawQuery();
      String path = exchange.getRequestURI().getPath();
      requests.add(exchange.getRequestMethod() + " " + path + (query == null ? "" : "?" + query));
      byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    server.start();
    apiClient = new ApiClient().setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void testTerminate() {
    DiscoveryServiceClient mockDiscoveryServiceClient = mock(DiscoveryServiceClient.class);
    ApiClient mockApiClient = mock(ApiClient.class);
    // obj meta with disable cleanup annotation
    V1ObjectMeta objMetaWithAnnotation = new V1ObjectMeta().name("test-job-name")
      .putAnnotationsItem(KubeTwillRunnerService.RUNTIME_CLEANUP_DISABLED, "true");
    CompletableFuture<Void> startupTaskCompletion = new CompletableFuture<>();
    KubeTwillController controller = new KubeTwillController("default", null, mockDiscoveryServiceClient,
                                                             mockApiClient, V1Job.class, objMetaWithAnnotation,
                                                             startupTaskCompletion);
    V1JobStatus jobStatus = new V1JobStatus();
    jobStatus.setFailed(1);
    controller.setJobStatus(jobStatus);

    Future<? extends ServiceController> terminateFuture = controller.terminate();
    Assert.assertTrue(terminateFuture.isDone());
  }

  @Test
  public void testRestartAllInstances() throws Exception {
    V1ObjectMeta meta = new V1ObjectMeta().name("test-dep").putLabelsItem("cdap.app", "app1");
    KubeTwillController controller = new KubeTwillController(
        "default", null, mock(DiscoveryServiceClient.class), apiClient, V1Deployment.class, meta,
        CompletableFuture.completedFuture(null));

    String result = controller.restartAllInstances("runnable").get(10, TimeUnit.SECONDS);
    Assert.assertEquals("runnable", result);
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /api/v1/namespaces/default/pods?")));
  }

  @Test
  public void testRestartInstanceAndInstances() throws Exception {
    V1ObjectMeta meta = new V1ObjectMeta().name("test-sts").putLabelsItem("cdap.app", "app1");
    KubeTwillController controller = new KubeTwillController(
        "default", null, mock(DiscoveryServiceClient.class), apiClient, V1StatefulSet.class, meta,
        CompletableFuture.completedFuture(null));

    Assert.assertEquals("runnable",
        controller.restartInstance("runnable", 0, "uid-1").get(10, TimeUnit.SECONDS));
    Assert.assertEquals("runnable",
        controller.restartInstances("runnable", Collections.singleton(0)).get(10, TimeUnit.SECONDS));
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /api/v1/namespaces/default/pods/test-sts-0")));
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /api/v1/namespaces/default/pods?labelSelector=")));
  }

  @Test
  public void testTerminateDeployment() throws Exception {
    V1ObjectMeta meta = new V1ObjectMeta().name("test-dep").putLabelsItem("cdap.app", "app1");
    KubeTwillController controller = new KubeTwillController(
        "default", null, mock(DiscoveryServiceClient.class), apiClient, V1Deployment.class, meta,
        CompletableFuture.completedFuture(null));

    controller.terminate().get(10, TimeUnit.SECONDS);
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /apis/apps/v1/namespaces/default/deployments/test-dep")));
  }

  @Test
  public void testTerminateStatefulSetDeletesPvcs() throws Exception {
    V1ObjectMeta meta = new V1ObjectMeta().name("test-sts").putLabelsItem("cdap.app", "app1");
    KubeTwillController controller = new KubeTwillController(
        "default", null, mock(DiscoveryServiceClient.class), apiClient, V1StatefulSet.class, meta,
        CompletableFuture.completedFuture(null));

    controller.terminate().get(10, TimeUnit.SECONDS);
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /apis/apps/v1/namespaces/default/statefulsets/test-sts")));
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /api/v1/namespaces/default/persistentvolumeclaims?")));
  }

  @Test
  public void testTerminateJobDeletesJob() throws Exception {
    V1ObjectMeta meta = new V1ObjectMeta().name("test-job").putLabelsItem("cdap.app", "app1");
    KubeTwillController controller = new KubeTwillController(
        "default", null, mock(DiscoveryServiceClient.class), apiClient, V1Job.class, meta,
        CompletableFuture.completedFuture(null));
    V1JobStatus jobStatus = new V1JobStatus().succeeded(1);
    controller.setJobStatus(jobStatus);

    controller.terminate().get(10, TimeUnit.SECONDS);
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
    while (requests.isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    Assert.assertTrue(requests.stream().anyMatch(
        r -> r.startsWith("DELETE /apis/batch/v1/namespaces/default/jobs/test-job")));
  }
}
