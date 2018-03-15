/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.runtime.monitor;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

/**
 * Runtime Monitor Test
 */
public class RuntimeMonitorTest {

  @Test
  public void testRunTimeMonitor() throws Exception {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 8000), 0);
    httpServer.createContext("/v3/runtime/metadata", new HttpHandler() {
      public void handle(HttpExchange exchange) throws IOException {
        byte[] response = "{\"success\": true}".getBytes();
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
      }
    });
    httpServer.start();

    try {
      ConnectionConfig connectionConfig = ConnectionConfig.builder()
        .setHostname("127.0.0.1")
        .setPort(8000)
        .setSSLEnabled(false)
        .build();
      ClientConfig.Builder clientConfigBuilder = ClientConfig.builder()
        .setDefaultReadTimeout(15000)
        .setConnectionConfig(connectionConfig);

      RuntimeMonitor runtimeMonitor = new RuntimeMonitor("runtimeId", 1000, clientConfigBuilder.build(), 1, "test");
      runtimeMonitor.startAndWait();
      Thread.sleep(5000);
      runtimeMonitor.stopAndWait();

    } finally {
      httpServer.stop(0);
    }
  }
}
