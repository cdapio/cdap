/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.sun.net.httpserver.HttpServer;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.kubernetes.client.openapi.ApiClient;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for {@link PodKillerTask}.
 */
public class PodKillerTaskTest {

  @Test
  public void testKillPods() throws Exception {
    List<String> requests = new CopyOnWriteArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    try {
      server.createContext("/", exchange -> {
        String query = exchange.getRequestURI().getRawQuery();
        requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath()
            + (query == null ? "" : "?" + query));
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(body);
        }
      });
      server.start();

      ApiClient apiClient = new ApiClient()
          .setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
      PodKillerTask task = new PodKillerTask("default", "cdap.container=preview", 1000,
                                             () -> apiClient);
      Assert.assertEquals(1000L, task.run(null));
      Assert.assertTrue(requests.stream().anyMatch(
          r -> r.startsWith("DELETE /api/v1/namespaces/default/pods?")
              && r.contains("labelSelector=cdap.container%3Dpreview")));
    } finally {
      server.stop(0);
    }
  }

  @Ignore("Requires a running kubernetes cluster")
  @Test
  public void test() {
    PodKillerTask task = new PodKillerTask("default", "cdap.container=preview", 1000,
                                           new DefaultApiClientFactory(10, 10));
    task.run(new MasterEnvironmentContext() {
      @Override
      public LocationFactory getLocationFactory() {
        throw new UnsupportedOperationException("LocationFactory is not supported");
      }

      @Override
      public Map<String, String> getConfigurations() {
        return Collections.emptyMap();
      }

      @Override
      public String[] getRunnableArguments(Class<? extends MasterEnvironmentRunnable> runnableClass,
                                           String... runnableArgs) {
        throw new UnsupportedOperationException("Master environment runnable execution is not supported");
      }
    });
  }
}
