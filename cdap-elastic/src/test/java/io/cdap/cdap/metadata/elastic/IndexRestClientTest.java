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

package io.cdap.cdap.metadata.elastic;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpHost;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link IndexRestClient}.
 */
public class IndexRestClientTest {

  @Test
  public void testParseHosts() {
    Map<String, List<HttpHost>> validCases = new LinkedHashMap<>();
    validCases.put("es", Collections.singletonList(new HttpHost("es", 9200, "http")));
    validCases.put("es:9201", Collections.singletonList(new HttpHost("es", 9201, "http")));
    validCases.put("http://es", Collections.singletonList(new HttpHost("es", 9200, "http")));
    validCases.put("https://es", Collections.singletonList(new HttpHost("es", 9243, "https")));
    validCases.put("https://es:8443",
        Collections.singletonList(new HttpHost("es", 8443, "https")));
    validCases.put(" host1 , https://host2 , , host3:9300 ", Arrays.asList(
        new HttpHost("host1", 9200, "http"),
        new HttpHost("host2", 9243, "https"),
        new HttpHost("host3", 9300, "http")));

    for (Map.Entry<String, List<HttpHost>> entry : validCases.entrySet()) {
      Assert.assertEquals("Input: " + entry.getKey(),
          entry.getValue(), IndexRestClient.parseHosts(entry.getKey()));
    }

    for (String invalid : Arrays.asList("", " ,  , ", "es:badport", "http://es:9200/")) {
      try {
        IndexRestClient.parseHosts(invalid);
        Assert.fail("Expected IllegalArgumentException for '" + invalid + "'");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  @Test
  public void testPathSegmentAndQueryParameterEncoding() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (ServerSocket server = new ServerSocket(0)) {
      final Future<String> requestLineFuture = executor.submit(() -> {
        try (Socket socket = server.accept();
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = socket.getOutputStream()) {
          final String requestLine = reader.readLine();
          String header;
          while ((header = reader.readLine()) != null && !header.isEmpty()) {
            // drain request headers
          }
          byte[] response = ("HTTP/1.1 200 OK\r\n"
              + "Content-Type: application/json\r\n"
              + "Content-Length: 2\r\n"
              + "Connection: close\r\n\r\n{}").getBytes(StandardCharsets.UTF_8);
          out.write(response);
          out.flush();
          return requestLine;
        }
      });

      String docId = "dataset:namespace=ns,dataset=a/b c%d+eä";
      Map<String, String> params = new LinkedHashMap<>();
      params.put("filter", "k=v+1 2/3%4");
      params.put("refresh", "wait_for");

      try (IndexRestClient client = new IndexRestClient(
          "localhost:" + server.getLocalPort(), null, null, true)) {
        IndexRestClient.Response response = client.execute(
            "GET", Arrays.asList("cdap.metadata", "_doc", docId), params, (JsonObject) null);
        Assert.assertEquals(200, response.getStatus());
      }

      String expectedRequestLine = "GET /cdap.metadata/_doc/"
          + "dataset%3Anamespace%3Dns%2Cdataset%3Da%2Fb%20c%25d%2Be%C3%A4"
          + "?filter=k%3Dv%2B1%202%2F3%254&refresh=wait_for HTTP/1.1";
      Assert.assertEquals(expectedRequestLine, requestLineFuture.get(5, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testBasicAuthenticationHeader() throws IOException {
    AtomicReference<String> receivedAuth = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      while (exchange.getRequestBody().read() >= 0) {
        // drain request body so HttpServer keeps the persistent connection clean
      }
      String auth = exchange.getRequestHeaders().getFirst("Authorization");
      if (auth == null) {
        byte[] challenge = "{\"error\":\"unauthorized\"}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"opensearch\"");
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(401, challenge.length);
        exchange.getResponseBody().write(challenge);
        exchange.close();
        return;
      }
      receivedAuth.set(auth);
      byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    try {
      try (IndexRestClient authClient = new IndexRestClient(
          "localhost:" + server.getAddress().getPort(), "admin", "s3cr:t", true)) {
        IndexRestClient.Response response = authClient.execute("GET", "_cluster", "health");
        Assert.assertEquals(200, response.getStatus());
        String expectedHeader = "Basic " + Base64.getEncoder()
            .encodeToString("admin:s3cr:t".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(expectedHeader, receivedAuth.get());
      }

      receivedAuth.set(null);
      try (IndexRestClient anonClient = new IndexRestClient(
          "localhost:" + server.getAddress().getPort(), null, null, true)) {
        IndexRestClient.Response response = anonClient.execute("GET", "_cluster", "health");
        Assert.assertEquals(401, response.getStatus());
        Assert.assertNull(receivedAuth.get());
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testFailoverOnlyOnConnectFailure() throws IOException {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }

    AtomicInteger firstHostRequests = new AtomicInteger();
    HttpServer failingHost = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    failingHost.createContext("/", exchange -> {
      firstHostRequests.incrementAndGet();
      byte[] body = "{\"error\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(500, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });

    AtomicInteger secondHostRequests = new AtomicInteger();
    HttpServer healthyHost = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    healthyHost.createContext("/", exchange -> {
      secondHostRequests.incrementAndGet();
      byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });

    failingHost.start();
    healthyHost.start();
    try {
      // Connect refusal on the first host fails over to the second host.
      try (IndexRestClient client = new IndexRestClient(
          "localhost:" + closedPort + ",localhost:" + healthyHost.getAddress().getPort(),
          null, null, true)) {
        IndexRestClient.Response response = client.execute("POST", "_bulk");
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(1, secondHostRequests.get());
      }

      // HTTP 500 from an established connection must NOT retry on the second host.
      secondHostRequests.set(0);
      try (IndexRestClient client = new IndexRestClient(
          "localhost:" + failingHost.getAddress().getPort()
              + ",localhost:" + healthyHost.getAddress().getPort(),
          null, null, true)) {
        IndexRestClient.Response response = client.execute("POST", "_bulk");
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(1, firstHostRequests.get());
        Assert.assertEquals(0, secondHostRequests.get());
      }
    } finally {
      failingHost.stop(0);
      healthyHost.stop(0);
    }
  }

  @Test
  public void testAllHostsUnreachable() throws IOException {
    int closedPort1;
    int closedPort2;
    try (ServerSocket socket1 = new ServerSocket(0); ServerSocket socket2 = new ServerSocket(0)) {
      closedPort1 = socket1.getLocalPort();
      closedPort2 = socket2.getLocalPort();
    }
    try (IndexRestClient client = new IndexRestClient(
        "localhost:" + closedPort1 + ",localhost:" + closedPort2, null, null, true)) {
      client.execute("GET", "_cluster", "health");
      Assert.fail("Expected the request to fail when no host is reachable");
    } catch (ConnectException e) {
      // the failure of the other host is kept
      Assert.assertEquals(1, e.getSuppressed().length);
    }
  }
}
