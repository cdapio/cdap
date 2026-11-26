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

package io.cdap.cdap.master.environment.k8s;

import static org.mockito.Mockito.mock;

import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.kubernetes.client.openapi.ApiClient;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link KubeJobCleaner}.
 */
public class KubeJobCleanerTest {

  @Test
  public void testCleanCompletedJobs() throws Exception {
    List<String> requests = new CopyOnWriteArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    try {
      server.createContext("/", exchange -> {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getRawQuery();
        requests.add(method + " " + path + (query == null ? "" : "?" + query));
        String json;
        if ("GET".equals(method) && "/apis/batch/v1/jobs".equals(path)) {
          json = "{\"metadata\":{},\"items\":["
              + "{\"metadata\":{\"name\":\"completed-job\",\"namespace\":\"ns1\"},\"status\":{\"succeeded\":1}},"
              + "{\"metadata\":{\"name\":\"running-job\",\"namespace\":\"ns1\"},\"status\":{\"active\":1}}"
              + "]}";
        } else {
          json = "{}";
        }
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      });
      server.start();

      ApiClient apiClient = new ApiClient()
          .setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
      KubeJobCleaner cleaner = new KubeJobCleaner("cdap.app=test", 10, 5, () -> apiClient);
      long delay = cleaner.run(mock(MasterEnvironmentContext.class));

      Assert.assertEquals(TimeUnit.MINUTES.toMillis(5), delay);
      Assert.assertTrue(requests.stream().anyMatch(
          r -> r.startsWith("GET /apis/batch/v1/jobs?") && r.contains("limit=10")));
      Assert.assertTrue(requests.stream().anyMatch(
          r -> r.startsWith("DELETE /apis/batch/v1/namespaces/ns1/jobs/completed-job")));
      Assert.assertFalse(requests.stream().anyMatch(
          r -> r.contains("running-job")));
    } finally {
      server.stop(0);
    }
  }
}
