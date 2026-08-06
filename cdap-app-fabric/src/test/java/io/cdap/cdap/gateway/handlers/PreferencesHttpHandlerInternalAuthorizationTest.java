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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * FileFetcherHttpHandlerInternal's sibling gap: PreferencesHttpHandlerInternal exposed
 * instance/namespace/application preferences with no authorization check at all,
 * unlike PreferencesHttpHandler (the public equivalent), which calls
 * accessEnforcer.enforce(..., StandardPermission.GET) at every one of those same scopes.
 * Mirrors ConfigHandlerAuthorizationTest. InMemoryAccessController implements
 * AccessController, which itself extends AccessEnforcer, so it can be passed directly
 * as the handler's AccessEnforcer dependency (this handler, unlike ConfigHandler, takes
 * the plain AccessEnforcer interface rather than ContextAccessEnforcer).
 */
public class PreferencesHttpHandlerInternalAuthorizationTest {
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                        Principal.PrincipalType.USER);
  private static final NamespaceId OTHER_NAMESPACE = new NamespaceId("other-tenant-namespace");

  private static PreferencesHttpHandlerInternal preferencesHandler;

  HttpRequest request;
  HttpResponder responder;
  Exception exceptionThrown;

  @BeforeClass
  public static void setup() {
    StandardPermission[] requiredPermissions = new StandardPermission[] {StandardPermission.GET};

    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(Arrays.asList(requiredPermissions))));
    inMemoryAccessController.grant(Authorizable.fromEntityId(OTHER_NAMESPACE), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(Arrays.asList(requiredPermissions))));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();

    PreferencesService mockPreferencesService = mock(PreferencesService.class);
    when(mockPreferencesService.getPreferences())
        .thenReturn(new PreferencesDetail(Collections.emptyMap(), 0, false));
    when(mockPreferencesService.getPreferences(any(NamespaceId.class)))
        .thenReturn(new PreferencesDetail(Collections.emptyMap(), 0, false));
    ApplicationLifecycleService mockAppLifecycleService = mock(ApplicationLifecycleService.class);
    NamespaceQueryAdmin mockNamespaceQueryAdmin = mock(NamespaceQueryAdmin.class);

    preferencesHandler = new PreferencesHttpHandlerInternal(mockPreferencesService, mockAppLifecycleService,
        mockNamespaceQueryAdmin, inMemoryAccessController, authenticationContext);
  }

  @Before
  public void initializeVariables() {
    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
    exceptionThrown = null;
  }

  @Test
  public void testGetInstancePreferencesUnauthorized() {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      preferencesHandler.getInstancePreferences(request, responder);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull("an unprivileged caller must not be able to read instance-level "
        + "preferences via the internal endpoint", exceptionThrown);
  }

  @Test
  public void testGetInstancePreferencesAuthorized() {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    try {
      preferencesHandler.getInstancePreferences(request, responder);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNull("a caller with instance-level access must not be rejected", exceptionThrown);
  }

  @Test
  public void testGetNamespacePreferencesUnauthorized() {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      preferencesHandler.getNamespacePreferences(request, responder, OTHER_NAMESPACE.getNamespace(), false);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull("an unprivileged caller must not be able to read another "
        + "namespace's preferences via the internal endpoint", exceptionThrown);
  }

  @Test
  public void testGetNamespacePreferencesAuthorized() {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    try {
      preferencesHandler.getNamespacePreferences(request, responder, OTHER_NAMESPACE.getNamespace(), false);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNull("a caller with access to the namespace must not be rejected", exceptionThrown);
  }

  @Test
  public void testGetApplicationPreferencesUnauthorized() {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      preferencesHandler.getApplicationPreferences(request, responder, OTHER_NAMESPACE.getNamespace(),
          "some-app", false);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull("an unprivileged caller must not be able to read another "
        + "namespace's application preferences via the internal endpoint", exceptionThrown);
  }
}
