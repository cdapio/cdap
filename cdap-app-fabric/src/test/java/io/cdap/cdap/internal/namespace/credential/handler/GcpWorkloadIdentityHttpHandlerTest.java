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

package io.cdap.cdap.internal.namespace.credential.handler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.gson.Gson;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.EncryptionExemptionHook;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.http.AuthenticationChannelHandler;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.worker.sidecar.GcpTokenResponse;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.internal.credential.CredentialProviderService;
import io.cdap.cdap.internal.credential.handler.CredentialProviderHttpHandlerInternal;
import io.cdap.cdap.internal.namespace.credential.DefaultNamespaceCredentialProviderService;
import io.cdap.cdap.internal.namespace.credential.RemoteNamespaceCredentialProvider;
import io.cdap.cdap.internal.provision.DefaultSystemProvisionerContext;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.NamespaceWorkloadIdentity;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.P4saTokenEndpointAuth;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link GcpWorkloadIdentityHttpHandler},
 * {@link GcpWorkloadIdentityHttpHandlerInternal},
 * {@link DefaultNamespaceCredentialProviderService}, and
 * {@link RemoteNamespaceCredentialProvider}.
 */
public class GcpWorkloadIdentityHttpHandlerTest {

  private static final Gson GSON = new Gson();
  private static final String SYSTEM_P4SA =
      "service-123456@gcp-sa-autopush-datafusion.iam.gserviceaccount.com";
  private static final String CONFIGURED_CONSUMER_P4SA =
      "cloud-datafusion-p4sa@priyanshu-cdf.iam.gserviceaccount.com";
  private static final String SYSTEM_USER_KSA = "cdap-zero-p4sa-user-sa";

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private NettyHttpService httpService;

  @After
  public void tearDown() throws Exception {
    if (httpService != null) {
      httpService.stop();
      httpService = null;
    }
  }

  private CConfiguration createStrictConfig() throws Exception {
    File keyFile = tempFolder.newFile("tokenManagerKey.json");
    byte[] rawKey = new byte[32];
    new SecureRandom().nextBytes(rawKey);
    String json = String.format(
        "{\"encodedKey\":\"%s\",\"algorithm\":\"AES\",\"keyId\":1,\"expiration\":%d}",
        Base64.getEncoder().encodeToString(rawKey), Long.MAX_VALUE);
    Files.write(keyFile.toPath(), json.getBytes(StandardCharsets.UTF_8));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, keyFile.getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.setBoolean(
        "feature." + Feature.DATAPLANE_AUDIT_LOGGING.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    cConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);
    cConf.set(Constants.CredentialProvider.SYSTEM_P4SA_SERVICE_ACCOUNT, CONFIGURED_CONSUMER_P4SA);
    cConf.set(Constants.CredentialProvider.SYSTEM_P4SA_K8S_IDENTITY, SYSTEM_USER_KSA);
    return cConf;
  }

  @Test
  public void testPutBindEnforcesValidateIdentityAndBlocksGoogleManagedServiceAccounts()
      throws Exception {
    final CConfiguration cConf = createStrictConfig();
    InMemoryNamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    NamespaceMeta nsMeta = new NamespaceMeta.Builder()
        .setName("ns_secure")
        .setIdentity("ns-secure-ksa")
        .build();
    namespaceAdmin.create(NamespaceMeta.SYSTEM);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    namespaceAdmin.create(nsMeta);

    CredentialProviderService underlyingProvider = Mockito.mock(CredentialProviderService.class);
    CredentialProfileManager profileManager = Mockito.mock(CredentialProfileManager.class);
    CredentialIdentityManager identityManager = Mockito.mock(CredentialIdentityManager.class);
    Mockito.when(identityManager.get(Mockito.any())).thenReturn(Optional.empty());
    Mockito.when(identityManager.list(Mockito.anyString())).thenReturn(Collections.emptyList());

    ContextAccessEnforcer accessEnforcer = Mockito.mock(ContextAccessEnforcer.class);
    DefaultNamespaceCredentialProviderService service =
        new DefaultNamespaceCredentialProviderService(
            cConf, underlyingProvider, accessEnforcer,
            namespaceAdmin, profileManager, identityManager);

    GcpWorkloadIdentityHttpHandler handler = new GcpWorkloadIdentityHttpHandler(
        cConf, accessEnforcer, namespaceAdmin, identityManager,
        service, new NoOpMetricsCollectionService());

    httpService = new CommonNettyHttpServiceBuilder(
        cConf, "test-wi", new NoOpMetricsCollectionService(), false,
        auditLogContexts -> {}, new NoOpAeadCipher())
        .setHttpHandlers(handler)
        .build();
    httpService.start();
    String baseUrl = String.format("http://%s:%d",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    URL putUrl = new URL(baseUrl + "/v3/namespaces/ns_secure/credentials/workloadIdentity");

    String[] rejectedServiceAccounts = new String[] {
        SYSTEM_P4SA,
        "service-999@gcp-sa-datafusion.iam.gserviceaccount.com",
        "cdf-tenant@abc12345-tp.iam.gserviceaccount.com",
        CONFIGURED_CONSUMER_P4SA,
        CONFIGURED_CONSUMER_P4SA.toUpperCase()
    };

    for (String rejectedSa : rejectedServiceAccounts) {
      HttpResponse resp = HttpRequests.execute(HttpRequest.put(putUrl)
          .withBody(GSON.toJson(new NamespaceWorkloadIdentity(rejectedSa)))
          .build());
      Assert.assertEquals("Expected 400 for rejected SA: " + rejectedSa,
          400, resp.getResponseCode());
    }
    Mockito.verify(identityManager, Mockito.never()).create(Mockito.any(), Mockito.any());

    // Customer-owned GSA passes validation and is persisted on PUT.
    String customerSa = "cdf-ns-secure-sa@priyanshu-cdf.iam.gserviceaccount.com";
    HttpResponse okResp = HttpRequests.execute(HttpRequest.put(putUrl)
        .withBody(GSON.toJson(new NamespaceWorkloadIdentity(customerSa)))
        .build());
    Assert.assertEquals(200, okResp.getResponseCode());
    Mockito.verify(underlyingProvider, Mockito.times(1))
        .validateIdentity(Mockito.eq(NamespaceId.SYSTEM.getNamespace()),
            Mockito.any(CredentialIdentity.class),
            Mockito.any(CredentialProvisionContext.class));
    Mockito.verify(identityManager, Mockito.times(1))
        .create(Mockito.any(CredentialIdentityId.class), Mockito.any(CredentialIdentity.class));
  }

  @Test
  public void testSystemP4saUpsertTokenEndpointAuthPipelineAndDirectProvisionBlock()
      throws Exception {
    final CConfiguration cConf = createStrictConfig();
    InMemoryNamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.SYSTEM);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);

    CredentialProviderService underlyingProvider = Mockito.mock(CredentialProviderService.class);
    CredentialProfileManager profileManager = Mockito.mock(CredentialProfileManager.class);
    CredentialIdentityManager identityManager = Mockito.mock(CredentialIdentityManager.class);
    // Simulate identity already existing so startUp() exercises AlreadyExistsException -> update().
    Mockito.doThrow(new AlreadyExistsException("system:system-p4sa"))
        .when(identityManager).create(Mockito.any(), Mockito.any());

    Mockito.when(underlyingProvider.provisionSystemP4sa(
            Mockito.any(CredentialProvisionContext.class)))
        .thenAnswer(inv -> {
          SecurityRequestContext.enqueueAuditLogContext(
              new AuditLogContext.Builder()
                  .setAuditLoggingRequired(true)
                  .setAuditLogBody("provision-system-p4sa")
                  .build());
          return new ProvisionedCredential("p4sa-access-token-123",
              Instant.now().plusSeconds(3500));
        });

    DefaultNamespaceCredentialProviderService service =
        new DefaultNamespaceCredentialProviderService(
            cConf, underlyingProvider, Mockito.mock(ContextAccessEnforcer.class),
            namespaceAdmin, profileManager, identityManager);
    service.startAndWait();

    ArgumentCaptor<CredentialIdentityId> idCaptor =
        ArgumentCaptor.forClass(CredentialIdentityId.class);
    ArgumentCaptor<CredentialIdentity> identityCaptor =
        ArgumentCaptor.forClass(CredentialIdentity.class);
    Mockito.verify(identityManager, Mockito.times(1))
        .update(idCaptor.capture(), identityCaptor.capture());
    Assert.assertEquals(
        DefaultNamespaceCredentialProviderService.SYSTEM_P4SA_IDENTITY_NAME,
        idCaptor.getValue().getName());
    Assert.assertEquals(SYSTEM_USER_KSA, identityCaptor.getValue().getIdentity());
    Assert.assertEquals(CONFIGURED_CONSUMER_P4SA, identityCaptor.getValue().getSecureValue());

    List<AuditLogRequest> publishedAuditLogs = new CopyOnWriteArrayList<>();
    GcpWorkloadIdentityHttpHandlerInternal internalHandler =
        new GcpWorkloadIdentityHttpHandlerInternal(cConf, service);
    CredentialProviderHttpHandlerInternal genericInternalHandler =
        new CredentialProviderHttpHandlerInternal(underlyingProvider);

    httpService = new CommonNettyHttpServiceBuilder(
        cConf, Constants.Service.APP_FABRIC_HTTP, new NoOpMetricsCollectionService(), true,
        publishedAuditLogs::add, new NoOpAeadCipher())
        .setHttpHandlers(internalHandler, genericInternalHandler)
        .build();
    httpService.start();
    String baseUrl = String.format("http://%s:%d",
        httpService.getBindAddress().getHostName(), httpService.getBindAddress().getPort());
    String validHmac = P4saTokenEndpointAuth.computeToken(cConf);
    Assert.assertNotNull(validHmac);

    // 1. Missing or invalid X-CDAP-System-P4SA-Token header (including query-param-only) -> 403.
    HttpURLConnection noTokenConn =
        (HttpURLConnection) new URL(baseUrl + "/v3Internal/system/p4sa/token").openConnection();
    Assert.assertEquals(403, noTokenConn.getResponseCode());
    noTokenConn.disconnect();

    HttpResponse badHeaderResp = HttpRequests.execute(
        HttpRequest.get(new URL(baseUrl + "/v3Internal/system/p4sa/token"))
            .addHeader(P4saTokenEndpointAuth.HEADER_NAME, "forged-hmac")
            .build());
    Assert.assertEquals(403, badHeaderResp.getResponseCode());

    HttpURLConnection queryOnlyConn = (HttpURLConnection) new URL(
        baseUrl + "/v3Internal/system/p4sa/token?token=" + validHmac).openConnection();
    Assert.assertEquals(403, queryOnlyConn.getResponseCode());
    queryOnlyConn.disconnect();

    // 2. Direct generic REST provisioning of system:system-p4sa returns 403 Forbidden.
    HttpResponse directGenericResp = HttpRequests.execute(
        HttpRequest.post(new URL(
                baseUrl + "/v3Internal/namespaces/system/credentials/identities/system-p4sa/"
                    + "provision"))
            .withBody(GSON.toJson(new CredentialProvisionContext()))
            .build());
    Assert.assertEquals(403, directGenericResp.getResponseCode());

    // 3. Provisioner context exposes http://127.0.0.1:<port>/token and relays header to AppFabric.
    cConf.set(Constants.Provisioner.SYSTEM_PROPERTY_PREFIX + "gcp-dataproc.token.endpoint",
        baseUrl + "/v3Internal/system/p4sa/token");
    cConf.setLong(Constants.Provisioner.RELOAD_INTERVAL, 0L);
    File cConfXml = tempFolder.newFile("cConf.xml");
    try (FileOutputStream fos = new FileOutputStream(cConfXml)) {
      cConf.writeXml(fos);
    }
    CConfiguration fileBackedConf = CConfiguration.create(cConfXml.toURI().toURL());
    Constructor<DefaultSystemProvisionerContext> ctor =
        DefaultSystemProvisionerContext.class.getDeclaredConstructor(
            CConfiguration.class, String.class);
    ctor.setAccessible(true);
    DefaultSystemProvisionerContext provisionerContext =
        ctor.newInstance(fileBackedConf, "gcp-dataproc");
    provisionerContext.reloadProperties();
    String bridgeEndpoint = provisionerContext.getProperties().get("token.endpoint");
    Assert.assertTrue(bridgeEndpoint.startsWith("http://127.0.0.1:"));
    Assert.assertTrue(bridgeEndpoint.endsWith("/token"));
    Assert.assertFalse(bridgeEndpoint.contains(validHmac));

    final Logger httpExceptionLogger =
        (Logger) LoggerFactory.getLogger(HttpExceptionHandler.class);
    final Logger handlerLogger =
        (Logger) LoggerFactory.getLogger(GcpWorkloadIdentityHttpHandlerInternal.class);
    final Logger exemptionLogger =
        (Logger) LoggerFactory.getLogger(EncryptionExemptionHook.class);
    final Logger authChannelLogger =
        (Logger) LoggerFactory.getLogger(AuthenticationChannelHandler.class);
    Level prevExemptionLevel = exemptionLogger.getLevel();
    exemptionLogger.setLevel(Level.TRACE);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    httpExceptionLogger.addAppender(appender);
    handlerLogger.addAppender(appender);
    exemptionLogger.addAppender(appender);
    authChannelLogger.addAppender(appender);
    try {
      HttpResponse validResp = HttpRequests.execute(
          HttpRequest.get(new URL(bridgeEndpoint + "?scopes=cloud-platform")).build());
      Assert.assertEquals(200, validResp.getResponseCode());
      GcpTokenResponse tokenResponse =
          GSON.fromJson(validResp.getResponseBodyAsString(), GcpTokenResponse.class);
      Assert.assertEquals("Bearer", tokenResponse.tokenType);
      Assert.assertEquals("p4sa-access-token-123", tokenResponse.accessToken);
      Assert.assertTrue(tokenResponse.expiresInSecs > 0);

      // AuditLogRequest URI and logs never contain validHmac by construction.
      Assert.assertFalse(publishedAuditLogs.isEmpty());
      for (AuditLogRequest auditReq : publishedAuditLogs) {
        Assert.assertFalse("Secret HMAC leaked in AuditLogRequest URI: " + auditReq.getUri(),
            auditReq.getUri().contains(validHmac));
      }

      Mockito.when(underlyingProvider.provisionSystemP4sa(
              Mockito.any(CredentialProvisionContext.class)))
          .thenThrow(new RuntimeException("simulated STS outage"));

      HttpResponse errorResp = HttpRequests.execute(
          HttpRequest.get(new URL(bridgeEndpoint)).build());
      Assert.assertEquals(500, errorResp.getResponseCode());
      Assert.assertFalse(errorResp.getResponseBodyAsString().contains(validHmac));
      for (ILoggingEvent event : appender.list) {
        Assert.assertFalse("Secret HMAC leaked in log message: " + event.getFormattedMessage(),
            event.getFormattedMessage().contains(validHmac));
      }
    } finally {
      exemptionLogger.setLevel(prevExemptionLevel);
      httpExceptionLogger.detachAppender(appender);
      handlerLogger.detachAppender(appender);
      exemptionLogger.detachAppender(appender);
      authChannelLogger.detachAppender(appender);
      appender.stop();
      service.stopAndWait();
    }
  }

  @Test
  public void testRemoteNamespaceCredentialProviderStatusMapping() throws Exception {
    AtomicInteger statusCodeToReturn = new AtomicInteger(401);
    NettyHttpService stubServer = NettyHttpService.builder("stub-appfabric")
        .setHttpHandlers(new StubProvisionHandler(statusCodeToReturn))
        .build();
    stubServer.start();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    Cancellable reg = discoveryService.register(
        URIScheme.HTTP.createDiscoverable(
            Constants.Service.APP_FABRIC_HTTP, stubServer.getBindAddress()));
    try {
      RemoteClientFactory remoteClientFactory =
          new RemoteClientFactory(discoveryService, new NoOpInternalAuthenticator());
      RemoteNamespaceCredentialProvider remoteProvider =
          new RemoteNamespaceCredentialProvider(
              remoteClientFactory, new NoOpInternalAuthenticator());

      statusCodeToReturn.set(401);
      try {
        remoteProvider.provision("ns1", "cloud-platform");
        Assert.fail("Expected UnauthorizedException on HTTP 401");
      } catch (UnauthorizedException expected) {
        Assert.assertEquals(403, expected.getStatusCode());
      }

      statusCodeToReturn.set(403);
      try {
        remoteProvider.provision("ns1", "cloud-platform");
        Assert.fail("Expected UnauthorizedException on HTTP 403");
      } catch (UnauthorizedException expected) {
        Assert.assertEquals(403, expected.getStatusCode());
      }

      statusCodeToReturn.set(404);
      try {
        remoteProvider.provision("ns1", "cloud-platform");
        Assert.fail("Expected NotFoundException on HTTP 404");
      } catch (NotFoundException expected) {
        Assert.assertTrue(expected.getMessage().contains("ns1"));
      }

      statusCodeToReturn.set(400);
      try {
        remoteProvider.provision("ns1", "cloud-platform");
        Assert.fail("Expected CredentialProvisioningException on HTTP 400");
      } catch (CredentialProvisioningException expected) {
        Assert.assertTrue(expected.getMessage().contains("400"));
      }

      statusCodeToReturn.set(200);
      ProvisionedCredential cred = remoteProvider.provision("ns1", "cloud-platform");
      Assert.assertEquals("remote-nsa-token", cred.get());
    } finally {
      reg.cancel();
      stubServer.stop();
    }
  }

  @Path(Constants.Gateway.INTERNAL_API_VERSION_3)
  public static final class StubProvisionHandler extends AbstractHttpHandler {
    private final AtomicInteger statusCode;

    StubProvisionHandler(AtomicInteger statusCode) {
      this.statusCode = statusCode;
    }

    @GET
    @Path("/namespaces/{namespace-id}/credentials/workloadIdentity/provision")
    public void provision(io.netty.handler.codec.http.HttpRequest request,
        HttpResponder responder) {
      int code = statusCode.get();
      if (code == 200) {
        responder.sendJson(HttpResponseStatus.OK,
            GSON.toJson(new ProvisionedCredential("remote-nsa-token",
                Instant.now().plusSeconds(3600))));
      } else {
        responder.sendString(HttpResponseStatus.valueOf(code), "status-" + code);
      }
    }
  }
}
