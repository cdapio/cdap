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

import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigHandlerAuthorizationTest {
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                        Principal.PrincipalType.USER);

  private static ConfigHandler configHandler;

  HttpRequest request;
  HttpResponder responder;
  Exception exceptionThrown;

  @BeforeClass
  public static void setup() {
    StandardPermission[] requiredPermissions = new StandardPermission[] {StandardPermission.GET};

    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(Arrays.asList(requiredPermissions))));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();
    DefaultContextAccessEnforcer contextAccessEnforcer = new DefaultContextAccessEnforcer(authenticationContext,
                                                                                          inMemoryAccessController);

    ConfigService mockConfigService = mock(ConfigService.class);

    configHandler = new ConfigHandler(mockConfigService, contextAccessEnforcer);
  }

  @Before
  public void initializeVariables() {
    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
    exceptionThrown = null;
  }

  @Test
  public void testConfigCDAPUnauthorized() throws IOException {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      configHandler.configCDAP(request, responder, "json");
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull(exceptionThrown);
  }

  @Test
  public void testConfigCDAPAuthorized() throws IOException {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    try {
      configHandler.configCDAP(request, responder, "json");
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNull(exceptionThrown);
  }

  @Test
  public void testConfigHadoopUnauthorized() throws IOException {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      configHandler.configHadoop(request, responder, "json");
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull(exceptionThrown);
  }

  @Test
  public void testConfigHadoopAuthorized() throws IOException {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    try {
      configHandler.configHadoop(request, responder, "json");
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNull(exceptionThrown);
  }
}
