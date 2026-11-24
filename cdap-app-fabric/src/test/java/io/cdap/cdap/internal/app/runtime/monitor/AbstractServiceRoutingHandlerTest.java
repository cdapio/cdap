/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.sun.net.httpserver.HttpServer;
import io.cdap.http.BodyConsumer;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractServiceRoutingHandler}.
 */
public class AbstractServiceRoutingHandlerTest {

  private static final String SERVICE_NAME = "test.routing.service";
  private static HttpServer httpServer;
  private static InMemoryDiscoveryService discoveryService;

  @BeforeClass
  public static void startServer() throws Exception {
    httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    httpServer.createContext("/ok", exchange -> {
      byte[] body = "routed-ok".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    });
    httpServer.createContext("/empty404", exchange -> {
      exchange.sendResponseHeaders(404, -1);
      exchange.close();
    });
    httpServer.createContext("/echo500", exchange -> {
      byte[] err = "upstream-failure".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(500, err.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(err);
      }
    });
    httpServer.start();

    discoveryService = new InMemoryDiscoveryService();
    discoveryService.register(new Discoverable(SERVICE_NAME, httpServer.getAddress()));
  }

  @AfterClass
  public static void stopServer() {
    if (httpServer != null) {
      httpServer.stop(0);
    }
  }

  @Test
  public void testRouteServiceFinishedAndHandleErrorCloseResources() throws Exception {
    AbstractServiceRoutingHandler handler = new AbstractServiceRoutingHandler(discoveryService) { };

    AtomicReference<HttpResponseStatus> capturedStatus = new AtomicReference<>();
    AtomicReference<BodyProducer> capturedProducer = new AtomicReference<>();
    HttpResponder responder = createCapturingResponder(capturedStatus, capturedProducer);

    DefaultHttpRequest getReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/ok");
    handler.routeService(getReq, responder, SERVICE_NAME, "/ok");
    Assert.assertEquals(HttpResponseStatus.OK, capturedStatus.get());
    ByteBuf chunk = capturedProducer.get().nextChunk();
    Assert.assertEquals("routed-ok", chunk.toString(StandardCharsets.UTF_8));
    capturedProducer.get().finished();

    // Exercise handleError cleanup path on RelayBodyProducer.
    handler.routeService(getReq, responder, SERVICE_NAME, "/ok");
    capturedProducer.get().handleError(new IOException("downstream reset"));

    // Exercise 404 with null error stream and finished() cleanup.
    DefaultHttpRequest notFoundReq =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/empty404");
    handler.routeService(notFoundReq, responder, SERVICE_NAME, "/empty404");
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND, capturedStatus.get());
    ByteBuf emptyChunk = capturedProducer.get().nextChunk();
    Assert.assertEquals(0, emptyChunk.readableBytes());
    capturedProducer.get().finished();
  }

  @Test
  public void testRouteServiceWithBodyErrorResponse() throws Exception {
    AbstractServiceRoutingHandler handler = new AbstractServiceRoutingHandler(discoveryService) { };
    AtomicReference<HttpResponseStatus> capturedStatus = new AtomicReference<>();
    AtomicReference<BodyProducer> capturedProducer = new AtomicReference<>();
    HttpResponder responder = createCapturingResponder(capturedStatus, capturedProducer);

    DefaultHttpRequest postReq =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/echo500");
    BodyConsumer consumer = handler.routeServiceWithBody(postReq, responder, SERVICE_NAME, "/echo500");
    consumer.chunk(Unpooled.copiedBuffer("payload", StandardCharsets.UTF_8), responder);
    consumer.finished(responder);

    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, capturedStatus.get());
    ByteBuf errChunk = capturedProducer.get().nextChunk();
    Assert.assertEquals("upstream-failure", errChunk.toString(StandardCharsets.UTF_8));
    capturedProducer.get().finished();
  }

  @Test
  public void testResponseInfoClosesInputStreamOnErrorStatusAndDisconnects() throws Exception {
    AtomicBoolean inputClosed = new AtomicBoolean(false);
    InputStream failingCloseInput = new ByteArrayInputStream("ignored".getBytes(StandardCharsets.UTF_8)) {
      @Override
      public void close() throws IOException {
        inputClosed.set(true);
        throw new IOException("close error should be ignored");
      }
    };

    HttpURLConnection urlConn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(urlConn.getResponseCode()).thenReturn(502);
    Mockito.when(urlConn.getInputStream()).thenReturn(failingCloseInput);
    Mockito.when(urlConn.getErrorStream()).thenReturn(null);
    Mockito.when(urlConn.getHeaderFields()).thenReturn(Collections.emptyMap());

    Class<?> responseInfoClass = Class.forName(
        AbstractServiceRoutingHandler.class.getName() + "$ResponseInfo");
    Constructor<?> ctor =
        responseInfoClass.getDeclaredConstructor(String.class, HttpURLConnection.class);
    ctor.setAccessible(true);
    Closeable responseInfo = (Closeable) ctor.newInstance(SERVICE_NAME, urlConn);

    Assert.assertTrue(inputClosed.get());
    responseInfo.close();
    Mockito.verify(urlConn).disconnect();
  }

  private HttpResponder createCapturingResponder(
      AtomicReference<HttpResponseStatus> statusRef,
      AtomicReference<BodyProducer> producerRef) {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    Mockito.doAnswer(invocation -> {
      statusRef.set(invocation.getArgument(0));
      producerRef.set(invocation.getArgument(1));
      return null;
    }).when(responder).sendContent(
        Mockito.any(HttpResponseStatus.class),
        Mockito.any(BodyProducer.class),
        Mockito.any(HttpHeaders.class));
    return responder;
  }
}
