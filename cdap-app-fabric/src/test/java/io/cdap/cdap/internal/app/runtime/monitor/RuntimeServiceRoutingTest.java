/*
 * Copyright © 2020-2023 Cask Data, Inc.
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
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit test for the {@link RuntimeServiceRoutingHandler}.
 */
public class RuntimeServiceRoutingTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String MOCK_SERVICE = "mock";
  private static final Gson GSON = new Gson();

  private Injector injector;
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;
  private NettyHttpService mockService;
  private Cancellable mockServiceCancellable;
  private MockRemoteAuthenticatorProvider mockRemoteAuthenticatorProvider;

  @Before
  public void beforeTest() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    mockRemoteAuthenticatorProvider = new MockRemoteAuthenticatorProvider();

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(RemoteAuthenticator.class).toProvider(mockRemoteAuthenticatorProvider);
          expose(RemoteAuthenticator.class);
        }
      },
      new LocalLocationModule(),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new RuntimeServerModule() {
        @Override
        protected void bindRequestValidator() {
          bind(RuntimeRequestValidator.class).toInstance((programRunId, request) -> {
            String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
            String expected = "Bearer " + Base64.getEncoder().encodeToString(
              Hashing.md5().hashString(programRunId.toString()).asBytes());
            if (!expected.equals(authHeader)) {
              throw new UnauthenticatedException("Program run " + programRunId + " is not authorized");
            }
            return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.COMPLETED));
          });
        }

        @Override
        protected void bindLogProcessor() {
          bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> {
          });
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      },
      new NoOpAuditLogModule(),
      new UserCredentialAeadEncryptionModule()
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    messagingService.createTopic(new DefaultTopicMetadata(NamespaceId.SYSTEM.topic("topic")));

    runtimeServer = injector.getInstance(RuntimeServer.class);
    runtimeServer.startAndWait();

    mockService = NettyHttpService.builder(MOCK_SERVICE)
      .setHost(InetAddress.getLocalHost().getCanonicalHostName())
      .setHttpHandlers(new PingHandler(), new MockServiceHandler())
      .build();

    mockService.start();
    mockServiceCancellable = injector.getInstance(DiscoveryService.class)
      .register(URIScheme.createDiscoverable(MOCK_SERVICE, mockService));
  }

  @After
  public void afterTest() throws Exception {
    mockServiceCancellable.cancel();
    mockService.stop();
    runtimeServer.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testGetAndDelete() throws IOException, UnauthorizedException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    mockRemoteAuthenticatorProvider.setAuthenticator(new MockRemoteAuthenticator(programRunId));

    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
      Constants.Service.RUNTIME,
      DefaultHttpRequestConfig.DEFAULT,
      Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");

    for (HttpMethod method : EnumSet.of(HttpMethod.GET, HttpMethod.DELETE)) {
      for (int status : Arrays.asList(200, 400, 404, 501)) {
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
  public void testPutAndPost() throws IOException, UnauthorizedException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    mockRemoteAuthenticatorProvider.setAuthenticator(new MockRemoteAuthenticator(programRunId));

    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
      Constants.Service.RUNTIME,
      DefaultHttpRequestConfig.DEFAULT,
      Constants.Gateway.INTERNAL_API_VERSION_3 + "/runtime/namespaces");
    String largeContent = Strings.repeat("Testing", 32768);

    for (String content : Arrays.asList("", "Small content", largeContent)) {
      for (HttpMethod method : EnumSet.of(HttpMethod.PUT, HttpMethod.POST)) {
        for (int status : Arrays.asList(200, 400, 404, 501)) {
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
  public void testUnauthorized() throws IOException, UnauthorizedException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app", "1.0").workflow("workflow").run(RunIds.generate());
    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class).createRemoteClient(
      Constants.Service.RUNTIME,
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

  @Test
  public void testRuntimeTokenExchangeHeadersAndContextRestoration() throws Exception {
    CConfiguration secConf = CConfiguration.create();
    secConf.setBoolean(Constants.Security.ENABLED, true);
    secConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);

    Injector secInjector = Guice.createInjector(
        new ConfigModule(secConf),
        new IOModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule());

    TokenManager tokenManager = secInjector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    try {
      Codec<AccessToken> tokenCodec = secInjector.getInstance(
          Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
      InternalAuthenticator internalAuthenticator =
          secInjector.getInstance(DefaultInternalAuthenticator.class);
      InternalAccessEnforcer internalAccessEnforcer =
          secInjector.getInstance(InternalAccessEnforcer.class);

      ProgramRunId runId = NamespaceId.DEFAULT.app("app", "1.0").workflow("wf")
          .run(RunIds.generate());
      long now = System.currentTimeMillis();
      AccessToken runtimeAccessToken = tokenManager.signIdentifier(new UserIdentity(
          GSON.toJson(runId), UserIdentity.IdentifierType.RUNTIME,
          Collections.emptyList(), now, now + TimeUnit.MINUTES.toMillis(5)));
      String callerTokenValue = Base64.getEncoder().encodeToString(
          tokenCodec.encode(runtimeAccessToken));
      String callerTokenHeader = Credential.CREDENTIAL_TYPE_INTERNAL + " " + callerTokenValue;

      AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
      AtomicReference<String> capturedBody = new AtomicReference<>();
      NettyHttpService captureService = NettyHttpService.builder("capture")
          .setHost(InetAddress.getLocalHost().getCanonicalHostName())
          .setHttpHandlers(new HeaderCaptureHandler(capturedHeaders, capturedBody))
          .build();
      captureService.start();
      Cancellable captureReg = injector.getInstance(DiscoveryService.class)
          .register(URIScheme.createDiscoverable("capture", captureService));

      try {
        RuntimeRequestValidator runtimeValidator = (id, req) ->
            ValidatedRuntimeRequest.runtimeToken(
                new ProgramRunInfo(ProgramRunStatus.RUNNING), runId, null);

        // Capture exact headers constructed before HttpURLConnection adds transport headers.
        AtomicReference<HttpHeaders> forwardedRequestHeaders = new AtomicReference<>();
        RuntimeServiceRoutingHandler handler = new RuntimeServiceRoutingHandler(
            injector.getInstance(DiscoveryServiceClient.class),
            runtimeValidator,
            internalAuthenticator) {
          @Override
          public void routeService(HttpRequest request, HttpResponder responder,
              String service, String path) throws Exception {
            forwardedRequestHeaders.set(request.headers());
            super.routeService(request, responder, service, path);
          }

          @Override
          public BodyConsumer routeServiceWithBody(HttpRequest request, HttpResponder responder,
              String service, String path) throws Exception {
            forwardedRequestHeaders.set(request.headers());
            return super.routeServiceWithBody(request, responder, service, path);
          }
        };

        String uri = String.format(
            "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/capture/echo",
            Constants.Gateway.INTERNAL_API_VERSION_3,
            runId.getNamespace(), runId.getApplication(), runId.getVersion(),
            runId.getType().getCategoryName(), runId.getProgram(), runId.getRun());

        Credential callerCred = new Credential(callerTokenValue, Credential.CredentialType.INTERNAL);
        SecurityRequestContext.reset();
        SecurityRequestContext.setUserId("caller-user");
        SecurityRequestContext.setUserIp("10.1.2.3");
        SecurityRequestContext.setUserCredential(callerCred);
        try {
          // 1. GET request with allowed + disallowed headers
          DefaultHttpRequest getReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, uri);
          getReq.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
          getReq.headers().set(HttpHeaderNames.IF_MODIFIED_SINCE, "Wed, 21 Oct 2015 07:28:00 GMT");
          getReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          getReq.headers().set(Constants.Security.Headers.USER_ID, "caller-user");
          getReq.headers().set(Constants.Security.Headers.USER_IP, "10.1.2.3");
          getReq.headers().set("X-Custom-Header", "should-not-forward");
          getReq.headers().set(HttpHeaderNames.COOKIE, "secret=1");

          HttpResponder responder = Mockito.mock(HttpResponder.class);
          handler.routeService(getReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");

          // Verify exact header set on the forwarded request object
          Set<String> expectedNames = new HashSet<>(Arrays.asList(
              HttpHeaderNames.CONTENT_TYPE.toString().toLowerCase(),
              HttpHeaderNames.IF_MODIFIED_SINCE.toString().toLowerCase(),
              Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase(),
              Constants.Security.Headers.USER_ID.toLowerCase()));
          Set<String> actualNames = new HashSet<>();
          for (String name : forwardedRequestHeaders.get().names()) {
            actualNames.add(name.toLowerCase());
          }
          Assert.assertEquals(expectedNames, actualNames);

          // Verify downstream received the exact exchanged headers and no leaked headers
          Map<String, String> received = capturedHeaders.get();
          Assert.assertNotNull(received);
          Assert.assertEquals("application/json", received.get("content-type"));
          Assert.assertEquals("Wed, 21 Oct 2015 07:28:00 GMT", received.get("if-modified-since"));
          Assert.assertEquals(Constants.Security.Authentication.RUNTIME_IDENTITY,
              received.get(Constants.Security.Headers.USER_ID.toLowerCase()));
          Assert.assertNull(received.get("x-custom-header"));
          Assert.assertNull(received.get("cookie"));
          Assert.assertNull(received.get(Constants.Security.Headers.USER_IP.toLowerCase()));

          String forwardedTokenHeader = received.get(
              Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase());
          Assert.assertNotNull(forwardedTokenHeader);
          Assert.assertNotEquals(callerTokenHeader, forwardedTokenHeader);
          Assert.assertTrue(forwardedTokenHeader.startsWith(Credential.CREDENTIAL_TYPE_INTERNAL + " "));
          String forwardedTokenValue = forwardedTokenHeader.substring(
              (Credential.CREDENTIAL_TYPE_INTERNAL + " ").length());
          // Must be accepted by InternalAccessEnforcer
          internalAccessEnforcer.enforce(InstanceId.SELF,
              new Principal(Constants.Security.Authentication.RUNTIME_IDENTITY,
                  Principal.PrincipalType.USER,
                  new Credential(forwardedTokenValue, Credential.CredentialType.INTERNAL)),
              StandardPermission.GET);

          // Verify SecurityRequestContext was restored
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 1b. GET request without optional headers forwards strictly the 2 internal auth headers
          DefaultHttpRequest minimalGetReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, uri);
          minimalGetReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          minimalGetReq.headers().set("X-Only-Caller", "drop");
          handler.routeService(minimalGetReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
          Set<String> minimalActualNames = new HashSet<>();
          for (String name : forwardedRequestHeaders.get().names()) {
            minimalActualNames.add(name.toLowerCase());
          }
          Assert.assertEquals(new HashSet<>(Arrays.asList(
              Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase(),
              Constants.Security.Headers.USER_ID.toLowerCase())), minimalActualNames);

          // 2. POST request with body: exchange happens before body is read, body relayed cleanly
          DefaultHttpRequest postReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.POST, uri);
          postReq.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
          postReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          postReq.headers().set("X-Extra", "drop");
          BodyConsumer consumer = handler.routeServiceWithBody(postReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
          consumer.chunk(Unpooled.copiedBuffer("hello-exchange", StandardCharsets.UTF_8), responder);
          consumer.finished(responder);
          Assert.assertEquals("hello-exchange", capturedBody.get());
          Assert.assertNull(capturedHeaders.get().get("x-extra"));
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 3. Exception during forwarding still leaves SecurityRequestContext restored
          String missingUri = uri.replace("/services/capture/", "/services/nonexistent-service/");
          DefaultHttpRequest missingReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
              io.netty.handler.codec.http.HttpMethod.GET, missingUri);
          missingReq.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, callerTokenHeader);
          try {
            handler.routeService(missingReq, responder,
                runId.getNamespace(), runId.getApplication(), runId.getVersion(),
                runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(),
                "nonexistent-service");
            Assert.fail("Expected ServiceUnavailableException");
          } catch (ServiceUnavailableException expected) {
            // expected
          }
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 4. Exception thrown inside InternalAuthenticator also restores SecurityRequestContext
          RuntimeServiceRoutingHandler failingAuthHandler = new RuntimeServiceRoutingHandler(
              injector.getInstance(DiscoveryServiceClient.class),
              runtimeValidator,
              headerSetter -> {
                Assert.assertNull(SecurityRequestContext.getUserId());
                Assert.assertNull(SecurityRequestContext.getUserIp());
                Assert.assertNull(SecurityRequestContext.getUserCredential());
                throw new RuntimeException("mint-failed");
              });
          try {
            failingAuthHandler.routeService(getReq, responder,
                runId.getNamespace(), runId.getApplication(), runId.getVersion(),
                runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
            Assert.fail("Expected RuntimeException");
          } catch (RuntimeException expected) {
            Assert.assertEquals("mint-failed", expected.getMessage());
          }
          Assert.assertEquals("caller-user", SecurityRequestContext.getUserId());
          Assert.assertEquals("10.1.2.3", SecurityRequestContext.getUserIp());
          Assert.assertEquals(callerCred, SecurityRequestContext.getUserCredential());

          // 5. Legacy-validated request forwards the original request headers unchanged
          RuntimeServiceRoutingHandler legacyHandler = new RuntimeServiceRoutingHandler(
              injector.getInstance(DiscoveryServiceClient.class),
              (id, req) -> ValidatedRuntimeRequest.legacy(
                  new ProgramRunInfo(ProgramRunStatus.RUNNING)),
              internalAuthenticator);
          capturedHeaders.set(null);
          legacyHandler.routeService(getReq, responder,
              runId.getNamespace(), runId.getApplication(), runId.getVersion(),
              runId.getType().getCategoryName(), runId.getProgram(), runId.getRun(), "capture");
          Map<String, String> legacyReceived = capturedHeaders.get();
          Assert.assertNotNull(legacyReceived);
          Assert.assertEquals("should-not-forward", legacyReceived.get("x-custom-header"));
          Assert.assertEquals(callerTokenHeader,
              legacyReceived.get(Constants.Security.Headers.RUNTIME_TOKEN.toLowerCase()));
        } finally {
          SecurityRequestContext.reset();
        }
      } finally {
        captureReg.cancel();
        captureService.stop();
      }
    } finally {
      tokenManager.stopAndWait();
    }
  }

  private static Map<String, String> copyHeaders(HttpHeaders headers) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, String> entry : headers.entries()) {
      map.put(entry.getKey().toLowerCase(), entry.getValue());
    }
    return map;
  }

  /**
   * Public HTTP handler that captures forwarded headers and request body for assertions.
   */
  public static final class HeaderCaptureHandler extends AbstractHttpHandler {

    private final AtomicReference<Map<String, String>> capturedHeaders;
    private final AtomicReference<String> capturedBody;

    public HeaderCaptureHandler(AtomicReference<Map<String, String>> capturedHeaders,
        AtomicReference<String> capturedBody) {
      this.capturedHeaders = capturedHeaders;
      this.capturedBody = capturedBody;
    }

    @Path("/echo")
    @GET
    public void echoGet(HttpRequest req, HttpResponder responder) {
      capturedHeaders.set(copyHeaders(req.headers()));
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/echo")
    @POST
    public void echoPost(FullHttpRequest req, HttpResponder responder) {
      capturedHeaders.set(copyHeaders(req.headers()));
      capturedBody.set(req.content().toString(StandardCharsets.UTF_8));
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  /**
   * A {@link RemoteAuthenticator} for testing. It generates a md5 signature from the program run id.
   */
  private static final class MockRemoteAuthenticator implements RemoteAuthenticator {

    private final ProgramRunId programRunId;

    private MockRemoteAuthenticator(ProgramRunId programRunId) {
      this.programRunId = programRunId;
    }

    @Override
    public String getName() {
      return "mock-remote-authenticator";
    }

    @Override
    public Credential getCredentials() {
      String credentialValue = Base64.getEncoder().encodeToString(Hashing.md5().hashString(programRunId.toString())
                                                                    .asBytes());
      return new Credential(credentialValue, Credential.CredentialType.EXTERNAL_BEARER);
    }

    /**
     * Returns the credentials for the authentication with scopes.
     */
    @Nullable
    @Override
    public Credential getCredentials(String scopes) throws IOException {
      return getCredentials();
    }
  }

  /**
   * A {@link RemoteAuthenticator} provider for testing.
   */
  private static final class MockRemoteAuthenticatorProvider implements Provider<RemoteAuthenticator> {

    private RemoteAuthenticator remoteAuthenticator = new NoOpRemoteAuthenticator();

    public void setAuthenticator(RemoteAuthenticator remoteAuthenticator) {
      this.remoteAuthenticator = remoteAuthenticator;
    }

    @Override
    public RemoteAuthenticator get() {
      return remoteAuthenticator;
    }
  }
}
