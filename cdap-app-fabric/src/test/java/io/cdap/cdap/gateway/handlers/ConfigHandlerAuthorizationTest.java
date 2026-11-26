/*
 * Copyright © 2022 Cask Data, Inc.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.authorization.InMemoryPrivilegeHolder;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigHandlerAuthorizationTest {
  private static final Principal MASTER_PRINCIPAL =
      new Principal("master", Principal.PrincipalType.USER);
  private static final Principal ACCESSOR_PRINCIPAL =
      new Principal("accessor", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL =
      new Principal("unprivileged", Principal.PrincipalType.USER);

  private static ConfigService mockConfigService;
  private static ConfigHandler configHandler;

  private HttpRequest request;
  private HttpResponder responder;

  @BeforeClass
  public static void setup() {
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();

    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), MASTER_PRINCIPAL,
        Collections.singleton(StandardPermission.UPDATE));
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ACCESSOR_PRINCIPAL,
        Collections.singleton(StandardPermission.GET));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();
    DefaultContextAccessEnforcer contextAccessEnforcer =
        new DefaultContextAccessEnforcer(authenticationContext, inMemoryAccessController);

    mockConfigService = mock(ConfigService.class);
    configHandler = new ConfigHandler(mockConfigService, contextAccessEnforcer);
  }

  @AfterClass
  public static void tearDownClass() {
    SecurityRequestContext.reset();
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();
  }

  @Before
  public void initializeVariables() {
    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
  }

  @Test
  public void testConfigCDAPUnauthorized() {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    Assert.assertThrows(UnauthorizedException.class,
        () -> configHandler.configCDAP(request, responder, "json"));
  }

  @Test
  public void testConfigCdapAccessorDenied() {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR_PRINCIPAL);
    Assert.assertThrows(UnauthorizedException.class,
        () -> configHandler.configCDAP(request, responder, "json"));
  }

  @Test
  public void testConfigCDAPAuthorized() throws IOException {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    configHandler.configCDAP(request, responder, "json");
    verify(mockConfigService).getCConf();
  }

  @Test
  public void testConfigHadoopUnauthorized() {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    Assert.assertThrows(UnauthorizedException.class,
        () -> configHandler.configHadoop(request, responder, "json"));
  }

  @Test
  public void testConfigHadoopAccessorDenied() {
    AuthenticationTestContext.actAsPrincipal(ACCESSOR_PRINCIPAL);
    Assert.assertThrows(UnauthorizedException.class,
        () -> configHandler.configHadoop(request, responder, "json"));
  }

  @Test
  public void testConfigHadoopAuthorized() throws IOException {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    configHandler.configHadoop(request, responder, "json");
    verify(mockConfigService).getHConf();
  }
}
