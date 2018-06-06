/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.status.OnConsoleStatusListener;
import ch.qos.logback.core.status.StatusManager;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.gateway.discovery.InMemoryRouteStore;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Unit-test for audit log.
 */
public class AuditLogTest {

  private static NettyRouter router;
  private static NettyHttpService httpService;
  private static Cancellable cancelDiscovery;
  private static URI baseURI;

  @BeforeClass
  public static void init() throws Exception {
    // Configure a log appender programmatically for the audit log
    TestLogAppender.addAppender(Constants.Router.AUDIT_LOGGER_NAME);
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Constants.Router.AUDIT_LOGGER_NAME)).setLevel(Level.TRACE);

    CConfiguration cConf = CConfiguration.create();
    SConfiguration sConf = SConfiguration.create();

    cConf.set(Constants.Router.ADDRESS, InetAddress.getLoopbackAddress().getHostAddress());
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    cConf.setBoolean(Constants.Router.ROUTER_AUDIT_LOG_ENABLED, true);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

    RouterServiceLookup serviceLookup = new RouterServiceLookup(
      cConf, discoveryService, new RouterPathLookup(),
      new InMemoryRouteStore(Collections.emptyMap()));

    router = new NettyRouter(cConf, sConf, InetAddress.getLoopbackAddress(), serviceLookup, new SuccessTokenValidator(),
                             new MockAccessTokenTransfomer(), discoveryService);
    router.startAndWait();

    httpService = NettyHttpService.builder("test").setHttpHandlers(new TestHandler()).build();
    httpService.start();

    cancelDiscovery = discoveryService.register(new Discoverable(Constants.Service.APP_FABRIC_HTTP,
                                                                 httpService.getBindAddress()));

    int port = router.getBoundAddress().orElseThrow(IllegalStateException::new).getPort();
    baseURI = URI.create(String.format("http://%s:%d", cConf.get(Constants.Router.ADDRESS), port));
  }

  @AfterClass
  public static void finish() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
    router.stopAndWait();
  }

  @Test
  public void testAuditLog() throws IOException {
    HttpURLConnection urlConn = createURLConnection("/get", HttpMethod.GET);
    Assert.assertEquals(200, urlConn.getResponseCode());
    urlConn.getInputStream().close();

    urlConn = createURLConnection("/put", HttpMethod.PUT);
    urlConn.getOutputStream().write("Test Put".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals("Test Put", new String(ByteStreams.toByteArray(urlConn.getInputStream()), "UTF-8"));
    urlConn.getInputStream().close();

    urlConn = createURLConnection("/post", HttpMethod.POST);
    urlConn.getOutputStream().write("Test Post".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals("Test Post", new String(ByteStreams.toByteArray(urlConn.getInputStream()), "UTF-8"));
    urlConn.getInputStream().close();

    urlConn = createURLConnection("/postHeaders", HttpMethod.POST);
    urlConn.setRequestProperty("user-id", "cdap");
    urlConn.getOutputStream().write("Post Headers".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(200, urlConn.getResponseCode());
    Assert.assertEquals("Post Headers", new String(ByteStreams.toByteArray(urlConn.getInputStream()), "UTF-8"));
    urlConn.getInputStream().close();

    List<String> loggedMessages = TestLogAppender.INSTANCE.getLoggedMessages();
    Assert.assertEquals(4, loggedMessages.size());

    Assert.assertTrue(loggedMessages.get(0).endsWith("\"GET /get HTTP/1.1\" - - 200 0 -"));
    Assert.assertTrue(loggedMessages.get(1).endsWith("\"PUT /put HTTP/1.1\" - Test Put 200 8 -"));
    Assert.assertTrue(loggedMessages.get(2).endsWith("\"POST /post HTTP/1.1\" - Test Post 200 9 Test Post"));
    Assert.assertTrue(
      loggedMessages.get(3).endsWith("\"POST /postHeaders HTTP/1.1\" {user-id=cdap} Post Headers 200 12 Post Headers"));
  }

  private HttpURLConnection createURLConnection(String path, HttpMethod method) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) baseURI.resolve(path).toURL().openConnection();
    urlConn.setRequestMethod(method.name());
    if (method.equals(HttpMethod.PUT) || method.equals(HttpMethod.POST)) {
      urlConn.setDoOutput(true);
    }

    return urlConn;
  }

  /**
   * A testing handler.
   */
  public static final class TestHandler extends AbstractHttpHandler {

    @Path("/get")
    @GET
    public void get(HttpRequest request, HttpResponder responder, @QueryParam("q") String query) {
      responder.sendString(HttpResponseStatus.OK, query);
    }

    @Path("/put")
    @PUT
    @AuditPolicy(AuditDetail.REQUEST_BODY)
    public void put(FullHttpRequest request, HttpResponder responder) {
      responder.sendContent(HttpResponseStatus.OK, request.content().retainedDuplicate(), EmptyHttpHeaders.INSTANCE);
    }

    @Path("/post")
    @POST
    @AuditPolicy({AuditDetail.REQUEST_BODY, AuditDetail.RESPONSE_BODY})
    public void post(FullHttpRequest request, HttpResponder responder) {
      responder.sendContent(HttpResponseStatus.OK, request.content().retainedDuplicate(), EmptyHttpHeaders.INSTANCE);
    }

    @Path("/postHeaders")
    @POST
    @AuditPolicy({AuditDetail.REQUEST_BODY, AuditDetail.RESPONSE_BODY, AuditDetail.HEADERS})
    public void postHeaders(FullHttpRequest request, HttpResponder responder, @HeaderParam("user-id") String userId) {
      responder.sendContent(HttpResponseStatus.OK, request.content().retainedDuplicate(), EmptyHttpHeaders.INSTANCE);
    }
  }

  /**
   * A log appender for holding audit log events.
   */
  private static final class TestLogAppender extends AppenderBase<ILoggingEvent> {

    private static final TestLogAppender INSTANCE = new TestLogAppender();

    private final Queue<String> loggedMessages = new ConcurrentLinkedQueue<>();

    static void addAppender(String loggerName) {
      LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
      Logger logger = loggerContext.getLogger(loggerName);


      // Check if the logger already contains the logAppender
      if (Iterators.contains(logger.iteratorForAppenders(), INSTANCE)) {
        return;
      }

      INSTANCE.setContext(loggerContext);

      // Display any errors during initialization of log appender to console
      StatusManager statusManager = loggerContext.getStatusManager();
      OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
      statusManager.add(onConsoleListener);

      INSTANCE.start();
      logger.addAppender(INSTANCE);
    }

    @Override
    protected void append(ILoggingEvent event) {
      loggedMessages.add(event.getFormattedMessage());
    }

    List<String> getLoggedMessages() {
      return new ArrayList<>(loggedMessages);
    }
  }
}
