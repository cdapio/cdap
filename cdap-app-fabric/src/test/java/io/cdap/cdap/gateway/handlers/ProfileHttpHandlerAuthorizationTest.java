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

package io.cdap.cdap.gateway.handlers;

import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.authorization.InMemoryPrivilegeHolder;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Verifies that {@link ProfileHttpHandler} blocks system profile reads for principals with no
 * visible namespace and redacts plaintext provisioner credentials for callers that cannot update
 * the profile.
 */
public class ProfileHttpHandlerAuthorizationTest {

  private static final String SECRET = "{\"type\":\"service_account\",\"private_key\":\"canary\"}";
  private static final String MACRO = "${secure(dataproc-key)}";

  private static final Principal ACCESSOR = new Principal("accessor", Principal.PrincipalType.USER);
  private static final Principal VIEWER = new Principal("viewer", Principal.PrincipalType.USER);
  private static final Principal ADMIN = new Principal("admin", Principal.PrincipalType.USER);

  private static final ProfileId SYSTEM_PROFILE = NamespaceId.SYSTEM.profile("dp");
  private static final ProfileId USER_PROFILE = new NamespaceId("userns").profile("dp-user");

  private InMemoryAccessController accessController;
  private ProfileService profileService;
  private ProvisioningService provisioningService;

  @Before
  public void setUp() throws Exception {
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();
    accessController = new InMemoryAccessController();

    // All three principals can read profiles, as CDF auto-approves system namespace reads.
    for (Principal principal : Arrays.asList(ACCESSOR, VIEWER, ADMIN)) {
      accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.PROFILE),
          principal, Collections.singleton(StandardPermission.LIST));
      accessController.grant(Authorizable.fromEntityId(SYSTEM_PROFILE), principal,
          Collections.singleton(StandardPermission.GET));
    }
    accessController.grant(Authorizable.fromEntityId(USER_PROFILE), VIEWER,
        Collections.singleton(StandardPermission.GET));
    // Only the admin can update profiles.
    accessController.grant(Authorizable.fromEntityId(SYSTEM_PROFILE), ADMIN,
        EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(USER_PROFILE), ADMIN,
        EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));

    profileService = Mockito.mock(ProfileService.class);
    provisioningService = Mockito.mock(ProvisioningService.class);
    Mockito.when(profileService.getProfile(SYSTEM_PROFILE))
        .thenReturn(profile("dp", EntityScope.SYSTEM, SECRET));
    Mockito.when(profileService.getProfile(USER_PROFILE))
        .thenReturn(profile("dp-user", EntityScope.USER, SECRET));
    Mockito.when(profileService.getProfiles(NamespaceId.SYSTEM, true))
        .thenReturn(Collections.singletonList(profile("dp", EntityScope.SYSTEM, SECRET)));
  }

  @After
  public void tearDown() {
    SecurityRequestContext.reset();
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();
  }

  @Test
  public void testSystemProfileReadsRequireVisibleNamespace() throws Exception {
    ProfileHttpHandler handler = handler(Collections.emptyList());
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getSystemProfiles(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getSystemProfile(null, responder, "dp"));
    Mockito.verifyNoInteractions(responder);
    Mockito.verify(profileService, Mockito.never()).getProfile(Mockito.any());
    Mockito.verify(profileService, Mockito.never()).getProfiles(Mockito.any(), Mockito.anyBoolean());
  }

  @Test
  public void testCredentialsRedactedForCallerWithoutUpdate() throws Exception {
    ProfileHttpHandler handler = handler(Collections.singletonList(NamespaceMeta.DEFAULT));
    AuthenticationTestContext.actAsPrincipal(VIEWER);

    String listBody = captureJson(responder -> handler.getSystemProfiles(null, responder));
    String getBody = captureJson(responder -> handler.getSystemProfile(null, responder, "dp"));
    String userBody = captureJson(
        responder -> handler.getProfile(null, responder, "userns", "dp-user"));

    for (String body : Arrays.asList(listBody, getBody, userBody)) {
      Assert.assertFalse(body, body.contains("canary"));
      Assert.assertTrue(body, body.contains(ProfileHttpHandler.REDACTED_VALUE));
      // Non-credential properties are returned unchanged.
      Assert.assertTrue(body, body.contains("my-project"));
    }
    // The stored profile is never rewritten with the redacted value.
    Mockito.verify(profileService, Mockito.never()).saveProfile(Mockito.any(), Mockito.any());
  }

  @Test
  public void testCredentialsReturnedToCallerWithUpdate() throws Exception {
    ProfileHttpHandler handler = handler(Collections.singletonList(NamespaceMeta.DEFAULT));
    AuthenticationTestContext.actAsPrincipal(ADMIN);

    String getBody = captureJson(responder -> handler.getSystemProfile(null, responder, "dp"));
    String userBody = captureJson(
        responder -> handler.getProfile(null, responder, "userns", "dp-user"));

    for (String body : Arrays.asList(getBody, userBody)) {
      Assert.assertTrue(body, body.contains("canary"));
      Assert.assertFalse(body, body.contains(ProfileHttpHandler.REDACTED_VALUE));
    }
  }

  @Test
  public void testSecureMacroIsNotRedacted() throws Exception {
    Mockito.when(profileService.getProfile(SYSTEM_PROFILE))
        .thenReturn(profile("dp", EntityScope.SYSTEM, MACRO));
    ProfileHttpHandler handler = handler(Collections.singletonList(NamespaceMeta.DEFAULT));
    AuthenticationTestContext.actAsPrincipal(VIEWER);

    String body = captureJson(responder -> handler.getSystemProfile(null, responder, "dp"));

    Assert.assertTrue(body, body.contains("secure(dataproc-key)"));
    Assert.assertFalse(body, body.contains(ProfileHttpHandler.REDACTED_VALUE));
  }

  private ProfileHttpHandler handler(List<NamespaceMeta> visibleNamespaces) throws Exception {
    NamespaceQueryAdmin namespaceAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(namespaceAdmin.list()).thenReturn(visibleNamespaces);
    return new ProfileHttpHandler(accessController, new AuthenticationTestContext(),
        profileService, provisioningService, namespaceAdmin);
  }

  private static Profile profile(String name, EntityScope scope, String accountKey) {
    ProvisionerInfo provisioner = new ProvisionerInfo("gcp-dataproc", "Custom", Arrays.asList(
        new ProvisionerPropertyValue("projectId", "my-project", true),
        new ProvisionerPropertyValue("accountKey", accountKey, true)));
    return new Profile(name, name, "test profile", scope, provisioner);
  }

  private interface HandlerCall {
    void call(HttpResponder responder) throws Exception;
  }

  private static String captureJson(HandlerCall call) throws Exception {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    call.call(responder);
    ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    Mockito.verify(responder).sendJson(Mockito.eq(HttpResponseStatus.OK), body.capture());
    return body.getValue();
  }
}
