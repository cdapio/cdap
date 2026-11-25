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

package io.cdap.cdap.app.runtime.spark;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Fast unit tests for {@link SparkTransactionClient} covering its {@code Stopwatch} polling loop.
 */
public class SparkTransactionClientTest {

  private static final TransactionCodec TX_CODEC = new TransactionCodec();

  private HttpServer server;
  private URI baseUri;
  private final AtomicInteger stageOneAttempts = new AtomicInteger();

  @Before
  public void setUp() throws Exception {
    final Transaction expectedTx = new Transaction(10L, 20L, new long[0], new long[0], -1L, TransactionType.LONG);
    final byte[] encodedTx = TX_CODEC.encode(expectedTx);

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/spark/stages/1/transaction", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if (stageOneAttempts.incrementAndGet() < 3) {
          exchange.sendResponseHeaders(404, -1);
          exchange.close();
          return;
        }
        exchange.sendResponseHeaders(200, encodedTx.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(encodedTx);
        }
      }
    });
    server.createContext("/spark/stages/2/transaction", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
      }
    });
    server.createContext("/spark/stages/3/transaction", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        byte[] err = "tx failed".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(410, err.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(err);
        }
      }
    });
    server.start();
    baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  @After
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void testRetrySuccessTimeoutAndFailure() throws Exception {
    SparkTransactionClient client = new SparkTransactionClient(baseUri, 10L);

    Transaction tx = client.getTransaction(1, 500L, TimeUnit.MILLISECONDS);
    Assert.assertEquals(20L, tx.getTransactionId());
    Assert.assertTrue(stageOneAttempts.get() >= 3);

    try {
      client.getTransaction(2, 50L, TimeUnit.MILLISECONDS);
      Assert.fail("Expected TimeoutException when stage remains 404");
    } catch (TimeoutException expected) {
      // expected
    }

    try {
      client.getTransaction(3, 500L, TimeUnit.MILLISECONDS);
      Assert.fail("Expected TransactionFailureException on HTTP 410");
    } catch (TransactionFailureException expected) {
      Assert.assertTrue(expected.getMessage().contains("tx failed"));
    }
  }
}
