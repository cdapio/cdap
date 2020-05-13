/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.AuthorizationException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Unit test for the {@link RuntimeServiceRoutingHandler}.
 */
public class RuntimeServiceRoutingTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String MOCK_SERVICE = "mock";

  private DiscoveryServiceClient discoveryServiceClient;
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;
  private NettyHttpService mockService;
  private Cancellable mockServiceCancellable;

  @Before
  public void beforeTest() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new RuntimeServerModule() {
        @Override
        protected void bindRequestValidator() {
          bind(RuntimeRequestValidator.class).toInstance((programRunId, request) -> {
            String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
            String expected = "test " + Base64.getEncoder().encodeToString(
              Hashing.md5().hashString(programRunId.toString()).asBytes());
            if (!expected.equals(authHeader)) {
              throw new AuthorizationException("Program run " + programRunId + " is not authorized");
            }
          });
        }

        @Override
        protected void bindLogProcessor() {
          bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> { });
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      }
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    messagingService.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic("topic")));

    runtimeServer = injector.getInstance(RuntimeServer.class);
    runtimeServer.startAndWait();

    mockService = NettyHttpService.builder(MOCK_SERVICE)
      .setHost(InetAddress.getLocalHost().getCanonicalHostName())
      .setHttpHandlers(new PingHandler(), new MockServiceHandler())
      .build();

    mockService.start();
    mockServiceCancellable = injector.getInstance(DiscoveryService.class)
      .register(URIScheme.createDiscoverable(MOCK_SERVICE, mockService));

    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
  }

  @After
  public void afterTest() throws Exception {
    mockServiceCancellable.cancel();
    mockService.stop();
    runtimeServer.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    RemoteAuthenticator.setDefaultAuthenticator(null);
  }

  @Test
  public void testGetAndDelete() throws IOException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    RemoteAuthenticator.setDefaultAuthenticator(new MockRemoteAuthenticator(programRunId));

    RemoteClient remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.RUNTIME,
                                                 DefaultHttpRequestConfig.DEFAULT,
                                                 Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");

    for (HttpMethod method : EnumSet.of(HttpMethod.GET, HttpMethod.DELETE)) {
      for (int status : Arrays.asList(200, 400, 404, 500)) {
        io.cdap.common.http.HttpRequest request =
          remoteClient.requestBuilder(method,
                                      String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/%s/%d",
                                                    programRunId.getNamespace(), programRunId.getApplication(),
                                                    programRunId.getVersion(), programRunId.getType().getCategoryName(),
                                                    programRunId.getProgram(), programRunId.getRun(),
                                                    MOCK_SERVICE, method.name().toLowerCase(), status))
            .build();

        HttpResponse response = remoteClient.execute(request);
        Assert.assertEquals(status, response.getResponseCode());
      }
    }
  }

  @Test
  public void testPutAndPost() throws IOException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    RemoteAuthenticator.setDefaultAuthenticator(new MockRemoteAuthenticator(programRunId));

    RemoteClient remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.RUNTIME,
                                                 DefaultHttpRequestConfig.DEFAULT,
                                                 Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");
    String largeContent = Strings.repeat("Testing", 32768);

    for (String content : Arrays.asList("", "Small content", largeContent)) {
      for (HttpMethod method : EnumSet.of(HttpMethod.PUT, HttpMethod.POST)) {
        for (int status : Arrays.asList(200, 400, 404, 500)) {
          io.cdap.common.http.HttpRequest request =
            remoteClient.requestBuilder(method,
                                        String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/%s/%d",
                                                      programRunId.getNamespace(), programRunId.getApplication(),
                                                      programRunId.getVersion(),
                                                      programRunId.getType().getCategoryName(),
                                                      programRunId.getProgram(), programRunId.getRun(),
                                                      MOCK_SERVICE, method.name().toLowerCase(), status))
              .withBody(content)
              .build();

          HttpResponse response = remoteClient.execute(request);
          Assert.assertEquals(status, response.getResponseCode());
          Assert.assertEquals(content, response.getResponseBodyAsString(StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test
  public void testUnauthorized() throws IOException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    RemoteClient remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.RUNTIME,
                                                 DefaultHttpRequestConfig.DEFAULT,
                                                 Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");
    io.cdap.common.http.HttpRequest request =
      remoteClient.requestBuilder(HttpMethod.GET,
                                  String.format("%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s/mock/%s/%d",
                                                programRunId.getNamespace(), programRunId.getApplication(),
                                                programRunId.getVersion(),
                                                programRunId.getType().getCategoryName(),
                                                programRunId.getProgram(), programRunId.getRun(),
                                                MOCK_SERVICE, "get", 200))
        .build();

    HttpResponse response = remoteClient.execute(request);
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, response.getResponseCode());
  }

  /**
   * A mock handler for testing service routing. It provides service endpoints for testing.
   */
  @Path("/mock")
  public static final class MockServiceHandler extends AbstractHttpHandler {

    @Path("/get/{status}")
    @GET
    public void get(HttpRequest request, HttpResponder responder, @PathParam("status") int statusCode) {
      responder.sendString(HttpResponseStatus.valueOf(statusCode), "Status is " + statusCode);
    }

    @Path("/delete/{status}")
    @DELETE
    public void delete(HttpRequest request, HttpResponder responder, @PathParam("status") int statusCode) {
      responder.sendString(HttpResponseStatus.valueOf(statusCode), "Status is " + statusCode);
    }

    @Path("/put/{status}")
    @PUT
    public void put(FullHttpRequest request, HttpResponder responder, @PathParam("status") int statusCode) {
      responder.sendString(HttpResponseStatus.valueOf(statusCode), request.content().toString(StandardCharsets.UTF_8));
    }

    @Path("/post/{status}")
    @POST
    public void post(FullHttpRequest request, HttpResponder responder, @PathParam("status") int statusCode) {
      responder.sendString(HttpResponseStatus.valueOf(statusCode), request.content().toString(StandardCharsets.UTF_8));
    }
  }

  /**
   * A {@link RemoteAuthenticator} for testing. It generates a md5 signature from the program run id.
   */
  private static final class MockRemoteAuthenticator extends RemoteAuthenticator {

    private final ProgramRunId programRunId;

    private MockRemoteAuthenticator(ProgramRunId programRunId) {
      this.programRunId = programRunId;
    }

    @Override
    public String getType() {
      return "test";
    }

    @Override
    public String getCredentials() {
      return Base64.getEncoder().encodeToString(Hashing.md5().hashString(programRunId.toString()).asBytes());
    }
  }
}
