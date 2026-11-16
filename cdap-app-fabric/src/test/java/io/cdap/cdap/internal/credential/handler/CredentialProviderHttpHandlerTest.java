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

package io.cdap.cdap.internal.credential.handler;

import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.internal.namespace.credential.GcpWorkloadIdentityUtil;
import io.cdap.cdap.proto.credential.CreateCredentialIdentityRequest;
import io.cdap.cdap.proto.credential.CreateCredentialProfileRequest;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.credential.ValidateIdentityRequest;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link CredentialProviderHttpHandler} and
 * {@link CredentialProviderHttpHandlerInternal}.
 */
public class CredentialProviderHttpHandlerTest {

  private static final Gson GSON = new Gson();
  private static final long MINUTE_MILLIS = 60 * 1000L;

  private ContextAccessEnforcer accessEnforcer;
  private NamespaceQueryAdmin namespaceQueryAdmin;
  private CredentialIdentityManager identityManager;
  private CredentialProfileManager profileManager;
  private CredentialProvider credentialProvider;
  private CredentialProviderHttpHandler handler;
  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;
  private InternalAccessEnforcer internalAccessEnforcer;

  @Before
  public void setUp() throws Exception {
    accessEnforcer = Mockito.mock(ContextAccessEnforcer.class);
    namespaceQueryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(namespaceQueryAdmin.exists(Mockito.any(NamespaceId.class))).thenReturn(true);
    identityManager = Mockito.mock(CredentialIdentityManager.class);
    profileManager = Mockito.mock(CredentialProfileManager.class);
    credentialProvider = Mockito.mock(CredentialProvider.class);
    handler = new CredentialProviderHttpHandler(
        accessEnforcer, namespaceQueryAdmin, identityManager, profileManager, credentialProvider);

    Injector injector = Guice.createInjector(
        new IOModule(), new ConfigModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules());
    tokenManager = injector.getInstance(TokenManager.class);
    accessTokenCodec = injector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
    tokenManager.startUp();
    internalAccessEnforcer = injector.getInstance(InternalAccessEnforcer.class);
  }

  @After
  public void tearDown() {
    if (tokenManager != null) {
      tokenManager.shutDown();
    }
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
  public void testPublicHandlerRejectsSystemAndInvalidNamespacesBeforeAccessEnforcer() {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    HttpRequest getReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    CredentialProfile profile = new CredentialProfile("gcp-wi", "desc", Collections.emptyMap());
    CreateCredentialProfileRequest createProfileReq =
        new CreateCredentialProfileRequest("prof", profile);
    CredentialIdentity identity = new CredentialIdentity("user_ns", "prof", "ksa", "gsa");
    CreateCredentialIdentityRequest createIdentityReq =
        new CreateCredentialIdentityRequest("id1", identity);
    ValidateIdentityRequest validateReq =
        new ValidateIdentityRequest(identity, new CredentialProvisionContext());

    for (String badNs : new String[]{NamespaceId.SYSTEM.getNamespace(), "!invalid"}) {
      assertThrows(BadRequestException.class, () ->
          handler.listProfiles(getReq, responder, badNs));
      assertThrows(BadRequestException.class, () ->
          handler.getProfile(getReq, responder, badNs, "prof"));
      assertThrows(BadRequestException.class, () ->
          handler.createProfile(
              jsonRequest(HttpMethod.POST, "/", createProfileReq), responder, badNs));
      assertThrows(BadRequestException.class, () ->
          handler.updateProfile(
              jsonRequest(HttpMethod.PUT, "/", profile), responder, badNs, "prof"));
      assertThrows(BadRequestException.class, () ->
          handler.deleteProfile(getReq, responder, badNs, "prof"));
      assertThrows(BadRequestException.class, () ->
          handler.listIdentities(getReq, responder, badNs));
      assertThrows(BadRequestException.class, () ->
          handler.getIdentity(getReq, responder, badNs, "id1"));
      assertThrows(BadRequestException.class, () ->
          handler.createIdentity(
              jsonRequest(HttpMethod.POST, "/", createIdentityReq), responder, badNs));
      assertThrows(BadRequestException.class, () ->
          handler.updateIdentity(
              jsonRequest(HttpMethod.PUT, "/", identity), responder, badNs, "id1"));
      assertThrows(BadRequestException.class, () ->
          handler.deleteIdentity(getReq, responder, badNs, "id1"));
      assertThrows(BadRequestException.class, () ->
          handler.validateIdentity(
              jsonRequest(HttpMethod.POST, "/", validateReq), responder, badNs));
    }

    // Verify accessEnforcer and managers were never invoked for system or invalid namespaces.
    Mockito.verifyZeroInteractions(
        accessEnforcer, profileManager, identityManager, credentialProvider);
  }

  @Test
  public void testValidateCreateUpdateIdentityRejectInvalidCrossNamespaceAndReservedProfile() {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    CredentialIdentity crossNsIdentity =
        new CredentialIdentity("other_ns", "prof", "ksa", "gsa");
    CredentialIdentity reservedWiIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        "cdap-instance-system-sa", "attacker@project.iam.gserviceaccount.com");
    CredentialIdentity nullProfileNsIdentity =
        new CredentialIdentity(null, "prof", "ksa", "gsa");
    CredentialIdentity emptyProfileNameIdentity =
        new CredentialIdentity("user_ns", "", "ksa", "gsa");
    CredentialIdentity emptyIdentityVal =
        new CredentialIdentity("user_ns", "prof", "", "gsa");
    CredentialIdentity invalidProfileNameIdentity =
        new CredentialIdentity("user_ns", "!bad_profile", "ksa", "gsa");

    for (CredentialIdentity badIdentity : new CredentialIdentity[]{
        crossNsIdentity, reservedWiIdentity, nullProfileNsIdentity,
        emptyProfileNameIdentity, emptyIdentityVal, invalidProfileNameIdentity}) {
      CreateCredentialIdentityRequest createReq =
          new CreateCredentialIdentityRequest("id1", badIdentity);
      ValidateIdentityRequest validateReq =
          new ValidateIdentityRequest(badIdentity, new CredentialProvisionContext());

      assertThrows(BadRequestException.class, () ->
          handler.createIdentity(
              jsonRequest(HttpMethod.POST, "/", createReq), responder, "user_ns"));
      assertThrows(BadRequestException.class, () ->
          handler.updateIdentity(
              jsonRequest(HttpMethod.PUT, "/", badIdentity), responder, "user_ns", "id1"));
      assertThrows(BadRequestException.class, () ->
          handler.validateIdentity(
              jsonRequest(HttpMethod.POST, "/", validateReq), responder, "user_ns"));
    }

    // Also verify null identity in ValidateIdentityRequest / CreateCredentialIdentityRequest.
    ValidateIdentityRequest nullValidateReq =
        new ValidateIdentityRequest(null, new CredentialProvisionContext());
    assertThrows(BadRequestException.class, () ->
        handler.validateIdentity(
            jsonRequest(HttpMethod.POST, "/", nullValidateReq), responder, "user_ns"));
    CreateCredentialIdentityRequest nullCreateReq =
        new CreateCredentialIdentityRequest("id1", null);
    assertThrows(BadRequestException.class, () ->
        handler.createIdentity(
            jsonRequest(HttpMethod.POST, "/", nullCreateReq), responder, "user_ns"));

    Mockito.verifyZeroInteractions(identityManager, credentialProvider);
  }

  @Test
  public void testUserNamespaceCrudAndSharedNonReservedSystemProfileSuccess() throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    CredentialProfile profile = new CredentialProfile("custom-type", "desc", Collections.emptyMap());
    CredentialProfileId userProfileId = new CredentialProfileId("user_ns", "user-prof");
    Mockito.when(profileManager.get(userProfileId)).thenReturn(Optional.of(profile));

    // 1. User namespace profile CRUD succeeds.
    handler.createProfile(
        jsonRequest(HttpMethod.POST, "/", new CreateCredentialProfileRequest("user-prof", profile)),
        responder, "user_ns");
    Mockito.verify(profileManager).create(
        Mockito.eq(userProfileId), Mockito.any(CredentialProfile.class));

    handler.getProfile(
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"),
        responder, "user_ns", "user-prof");

    // 2. User namespace identity referencing a non-reserved shared system profile succeeds.
    CredentialIdentity sharedSystemProfileIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), "custom-shared-profile", "user-ksa", "user-secret");
    CredentialIdentityId userIdentityId = new CredentialIdentityId("user_ns", "id1");

    handler.createIdentity(
        jsonRequest(HttpMethod.POST, "/",
            new CreateCredentialIdentityRequest("id1", sharedSystemProfileIdentity)),
        responder, "user_ns");
    Mockito.verify(identityManager).create(
        Mockito.eq(userIdentityId), Mockito.any(CredentialIdentity.class));

    handler.updateIdentity(
        jsonRequest(HttpMethod.PUT, "/", sharedSystemProfileIdentity),
        responder, "user_ns", "id1");
    Mockito.verify(identityManager).update(
        Mockito.eq(userIdentityId), Mockito.any(CredentialIdentity.class));

    CredentialProvisionContext context = new CredentialProvisionContext();
    handler.validateIdentity(
        jsonRequest(HttpMethod.POST, "/",
            new ValidateIdentityRequest(sharedSystemProfileIdentity, context)),
        responder, "user_ns");
    Mockito.verify(credentialProvider)
        .validateIdentity(Mockito.eq("user_ns"), Mockito.any(CredentialIdentity.class),
            Mockito.any(CredentialProvisionContext.class));
  }

  @Test
  public void testInternalHandlerEnforcesInternalAuthAndRejectsSystemNamespace() throws Exception {
    AtomicReference<Principal> currentPrincipal = new AtomicReference<>();
    AuthenticationContext authContext = currentPrincipal::get;
    CredentialProviderHttpHandlerInternal internalHandler =
        new CredentialProviderHttpHandlerInternal(
            credentialProvider, authContext, internalAccessEnforcer);
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    CredentialProvisionContext provisionContext = new CredentialProvisionContext();

    // Build a valid signed INTERNAL principal.
    long now = System.currentTimeMillis();
    UserIdentity internalIdentity = new UserIdentity(
        "system", UserIdentity.IdentifierType.INTERNAL,
        Collections.emptyList(), now, now + 5 * MINUTE_MILLIS);
    String signedInternalToken = Base64.getEncoder()
        .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(internalIdentity)));
    Principal validInternalPrincipal = new Principal(
        "system", Principal.PrincipalType.USER, null,
        new Credential(signedInternalToken, Credential.CredentialType.INTERNAL));

    // 1. System namespace and invalid namespace/name are rejected even with valid INTERNAL token.
    currentPrincipal.set(validInternalPrincipal);
    assertThrows(BadRequestException.class, () ->
        internalHandler.provisionCredential(
            jsonRequest(HttpMethod.POST, "/", provisionContext),
            responder, NamespaceId.SYSTEM.getNamespace(), "ns-gcp-wi-default"));
    assertThrows(BadRequestException.class, () ->
        internalHandler.provisionCredential(
            jsonRequest(HttpMethod.POST, "/", provisionContext),
            responder, "!invalid", "id1"));
    Mockito.verifyZeroInteractions(credentialProvider);

    // 2. Unauthenticated (null credential) or EXTERNAL credential fails with IllegalStateException.
    currentPrincipal.set(new Principal("alice", Principal.PrincipalType.USER, null, null));
    assertThrows(IllegalStateException.class, () ->
        internalHandler.provisionCredential(
            jsonRequest(HttpMethod.POST, "/", provisionContext),
            responder, "user_ns", "id1"));

    currentPrincipal.set(new Principal(
        "alice", Principal.PrincipalType.USER, null,
        new Credential(signedInternalToken, Credential.CredentialType.EXTERNAL)));
    assertThrows(IllegalStateException.class, () ->
        internalHandler.provisionCredential(
            jsonRequest(HttpMethod.POST, "/", provisionContext),
            responder, "user_ns", "id1"));

    // 3. Forged INTERNAL credential fails with AccessException.
    currentPrincipal.set(new Principal(
        "alice", Principal.PrincipalType.USER, null,
        new Credential("invalid", Credential.CredentialType.INTERNAL)));
    assertThrows(AccessException.class, () ->
        internalHandler.provisionCredential(
            jsonRequest(HttpMethod.POST, "/", provisionContext),
            responder, "user_ns", "id1"));
    Mockito.verifyZeroInteractions(credentialProvider);

    // 4. Valid signed INTERNAL credential on user namespace succeeds.
    currentPrincipal.set(validInternalPrincipal);
    ProvisionedCredential expectedCred =
        new ProvisionedCredential("short-lived-token", Instant.now().plusSeconds(3600));
    Mockito.when(credentialProvider.provision(
            Mockito.eq("user_ns"), Mockito.eq("id1"),
            Mockito.any(CredentialProvisionContext.class)))
        .thenReturn(expectedCred);

    internalHandler.provisionCredential(
        jsonRequest(HttpMethod.POST, "/", provisionContext),
        responder, "user_ns", "id1");
    Mockito.verify(responder).sendJson(
        Mockito.eq(HttpResponseStatus.OK), Mockito.anyString());
  }
}
