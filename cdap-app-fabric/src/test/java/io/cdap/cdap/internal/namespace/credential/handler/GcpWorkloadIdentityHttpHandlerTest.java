/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.internal.namespace.credential.DefaultNamespaceCredentialProviderService;
import io.cdap.cdap.internal.namespace.credential.GcpWorkloadIdentityUtil;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NamespaceWorkloadIdentity;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link GcpWorkloadIdentityHttpHandler},
 * {@link GcpWorkloadIdentityHttpHandlerInternal}, and
 * {@link DefaultNamespaceCredentialProviderService}.
 */
public class GcpWorkloadIdentityHttpHandlerTest {

  private static final Gson GSON = new Gson();

  private ContextAccessEnforcer accessEnforcer;
  private NamespaceAdmin namespaceAdmin;
  private CredentialIdentityManager identityManager;
  private CredentialProfileManager profileManager;
  private CredentialProvider credentialProvider;
  private MetricsCollectionService metricsCollectionService;
  private DefaultNamespaceCredentialProviderService namespaceCredentialService;
  private GcpWorkloadIdentityHttpHandler publicHandler;
  private GcpWorkloadIdentityHttpHandlerInternal internalHandler;

  @Before
  public void setUp() throws Exception {
    accessEnforcer = Mockito.mock(ContextAccessEnforcer.class);
    namespaceAdmin = Mockito.mock(NamespaceAdmin.class);
    identityManager = Mockito.mock(CredentialIdentityManager.class);
    profileManager = Mockito.mock(CredentialProfileManager.class);
    credentialProvider = Mockito.mock(CredentialProvider.class);
    metricsCollectionService = Mockito.mock(MetricsCollectionService.class);
    MetricsContext metricsContext = Mockito.mock(MetricsContext.class);
    Mockito.when(metricsCollectionService.getContext(Mockito.anyMap())).thenReturn(metricsContext);

    Constructor<DefaultNamespaceCredentialProviderService> ctor =
        DefaultNamespaceCredentialProviderService.class.getDeclaredConstructor(
            CConfiguration.class, CredentialProvider.class, ContextAccessEnforcer.class,
            NamespaceAdmin.class, CredentialProfileManager.class);
    ctor.setAccessible(true);
    namespaceCredentialService = ctor.newInstance(
        CConfiguration.create(), credentialProvider, accessEnforcer,
        namespaceAdmin, profileManager);

    publicHandler = new GcpWorkloadIdentityHttpHandler(
        accessEnforcer, namespaceAdmin, identityManager,
        namespaceCredentialService, metricsCollectionService);
    internalHandler = new GcpWorkloadIdentityHttpHandlerInternal(namespaceCredentialService);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private static <T extends Throwable> T assertThrows(
      Class<T> expectedType, ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      if (expectedType.isInstance(t)) {
        return expectedType.cast(t);
      }
      throw new AssertionError(
          "Expected " + expectedType.getName() + " but caught " + t.getClass().getName(), t);
    }
    throw new AssertionError("Expected " + expectedType.getName() + " to be thrown");
  }

  private static FullHttpRequest jsonRequest(HttpMethod method, String uri, Object body) {
    byte[] bytes = GSON.toJson(body).getBytes(StandardCharsets.UTF_8);
    return new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, method, uri, Unpooled.wrappedBuffer(bytes));
  }

  @Test
  public void testSystemAndInvalidNamespaceRejectedBeforeAccessEnforcer() {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    HttpRequest httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    NamespaceWorkloadIdentity wiPayload =
        new NamespaceWorkloadIdentity("user-sa@project.iam.gserviceaccount.com");

    for (String badNs : new String[]{NamespaceId.SYSTEM.getNamespace(), "!invalid"}) {
      assertThrows(BadRequestException.class, () ->
          publicHandler.validateIdentity(
              jsonRequest(HttpMethod.POST, "/", wiPayload), responder, badNs));
      assertThrows(BadRequestException.class, () ->
          publicHandler.getIdentity(httpReq, responder, badNs));
      assertThrows(BadRequestException.class, () ->
          publicHandler.createIdentity(
              jsonRequest(HttpMethod.PUT, "/", wiPayload), responder, badNs));
      assertThrows(BadRequestException.class, () ->
          publicHandler.deleteIdentity(httpReq, responder, badNs));
      assertThrows(BadRequestException.class, () ->
          internalHandler.provisionCredential(
              httpReq, responder, badNs, "https://www.googleapis.com/auth/cloud-platform"));
      assertThrows(CredentialProvisioningException.class, () ->
          namespaceCredentialService.provision(
              badNs, "https://www.googleapis.com/auth/cloud-platform"));
      assertThrows(IdentityValidationException.class, () ->
          namespaceCredentialService.validateIdentity(
              badNs, "user-sa@project.iam.gserviceaccount.com"));
    }

    // Verify accessEnforcer, namespaceAdmin, identityManager, and credentialProvider were never called.
    Mockito.verifyZeroInteractions(
        accessEnforcer, namespaceAdmin, identityManager, credentialProvider);
  }

  @Test
  public void testUserNamespaceWorkloadIdentityLifecycleAndProvisionSuccess() throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    HttpRequest httpReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    NamespaceId userNsId = new NamespaceId("user_ns");
    String ksaName = "user-ns-528b6910248f7b8";
    NamespaceMeta userNsMeta = new NamespaceMeta.Builder()
        .setName(userNsId)
        .setIdentity(ksaName)
        .build();
    Mockito.when(namespaceAdmin.get(userNsId)).thenReturn(userNsMeta);

    String gsaEmail = "user-sa@project.iam.gserviceaccount.com";
    NamespaceWorkloadIdentity wiPayload = new NamespaceWorkloadIdentity(gsaEmail);
    CredentialIdentityId systemStoredId = new CredentialIdentityId(
        NamespaceId.SYSTEM.getNamespace(),
        GcpWorkloadIdentityUtil.getWorkloadIdentityName(userNsId));
    CredentialIdentity storedIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        ksaName, gsaEmail);

    // 1. PUT createIdentity creates system:ns-gcp-wi-<hash> with the namespace KSA.
    Mockito.when(identityManager.get(systemStoredId)).thenReturn(Optional.empty());
    Mockito.when(identityManager.list(NamespaceId.SYSTEM.getNamespace()))
        .thenReturn(Collections.singletonList(systemStoredId));
    publicHandler.createIdentity(
        jsonRequest(HttpMethod.PUT, "/", wiPayload), responder, "user_ns");
    org.mockito.ArgumentCaptor<CredentialIdentity> identityCaptor =
        org.mockito.ArgumentCaptor.forClass(CredentialIdentity.class);
    Mockito.verify(identityManager).create(Mockito.eq(systemStoredId), identityCaptor.capture());
    Assert.assertEquals(
        NamespaceId.SYSTEM.getNamespace(), identityCaptor.getValue().getProfileNamespace());
    Assert.assertEquals(
        GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME, identityCaptor.getValue().getProfileName());
    Assert.assertEquals(ksaName, identityCaptor.getValue().getIdentity());
    Assert.assertEquals(gsaEmail, identityCaptor.getValue().getSecureValue());

    // 2. GET getIdentity returns the configured GSA.
    Mockito.when(identityManager.get(systemStoredId)).thenReturn(Optional.of(storedIdentity));
    publicHandler.getIdentity(httpReq, responder, "user_ns");
    Mockito.verify(responder).sendJson(
        Mockito.eq(HttpResponseStatus.OK), Mockito.eq(GSON.toJson(wiPayload)));

    // 3. POST validateIdentity validates through DefaultNamespaceCredentialProviderService.
    publicHandler.validateIdentity(
        jsonRequest(HttpMethod.POST, "/", wiPayload), responder, "user_ns");
    Mockito.verify(credentialProvider).validateIdentity(
        Mockito.eq(NamespaceId.SYSTEM.getNamespace()),
        Mockito.any(CredentialIdentity.class),
        Mockito.any(CredentialProvisionContext.class));

    // 4. GET /v3Internal/namespaces/user_ns/credentials/workloadIdentity/provision provisions token.
    ProvisionedCredential expectedToken =
        new ProvisionedCredential("provisioned-access-token", Instant.now().plusSeconds(3600));
    Mockito.when(credentialProvider.provision(
            Mockito.eq(NamespaceId.SYSTEM.getNamespace()),
            Mockito.eq(systemStoredId.getName()),
            Mockito.any(CredentialProvisionContext.class)))
        .thenReturn(expectedToken);
    internalHandler.provisionCredential(
        httpReq, responder, "user_ns", "https://www.googleapis.com/auth/cloud-platform");

    // 5. DELETE deleteIdentity removes system:ns-gcp-wi-<hash>.
    publicHandler.deleteIdentity(httpReq, responder, "user_ns");
    Mockito.verify(identityManager).delete(systemStoredId);
  }
}
