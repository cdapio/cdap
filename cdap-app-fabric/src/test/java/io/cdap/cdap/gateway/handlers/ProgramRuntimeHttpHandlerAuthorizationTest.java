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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.proto.NotRunningProgramLiveInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
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
import org.mockito.Matchers;

public class ProgramRuntimeHttpHandlerAuthorizationTest {
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                        Principal.PrincipalType.USER);
  private static final NamespaceId NAMESPACE_ID = new NamespaceId("ns");
  private static final ApplicationId APP_ID = NAMESPACE_ID.app("app");
  private static final ProgramId PROGRAM_ID = APP_ID.service("service");

  private static ProgramRuntimeHttpHandler programRuntimeHttpHandler;
  private static ProgramRuntimeService runtimeService;

  HttpRequest request;
  HttpResponder responder;
  Exception exceptionThrown;

  @BeforeClass
  public static void setup() throws ApplicationNotFoundException {
    StandardPermission[] requiredPermissions = new StandardPermission[] {StandardPermission.GET};

    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(PROGRAM_ID), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(Arrays.asList(requiredPermissions))));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();

    ProgramLifecycleService lifecycleService = mock(ProgramLifecycleService.class);
    Store store = mock(Store.class);
    runtimeService = mock(ProgramRuntimeService.class);
    NamespaceQueryAdmin namespaceQueryAdmin = mock(NamespaceQueryAdmin.class);

    when(store.getLatestApp(Matchers.any(ApplicationReference.class))).thenReturn(APP_ID);
    when(runtimeService.getLiveInfo(PROGRAM_ID)).thenReturn(new NotRunningProgramLiveInfo(PROGRAM_ID));

    programRuntimeHttpHandler = new ProgramRuntimeHttpHandler(lifecycleService, store, runtimeService,
                                                              namespaceQueryAdmin, inMemoryAccessController,
                                                              authenticationContext);
  }

  @Before
  public void initializeVariables() {
    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
    exceptionThrown = null;
    reset(runtimeService);
    when(runtimeService.getLiveInfo(PROGRAM_ID)).thenReturn(new NotRunningProgramLiveInfo(PROGRAM_ID));
  }

  @Test
  public void testLiveInfoUnauthorized() throws BadRequestException, ApplicationNotFoundException {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      programRuntimeHttpHandler.liveInfo(request, responder, NAMESPACE_ID.getNamespace(), APP_ID.getApplication(),
                                         ProgramType.SERVICE.getCategoryName(), PROGRAM_ID.getProgram());
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull(exceptionThrown);
    verify(runtimeService, never()).getLiveInfo(Matchers.any(ProgramId.class));
  }

  @Test
  public void testLiveInfoAuthorized() throws BadRequestException, ApplicationNotFoundException {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    try {
      programRuntimeHttpHandler.liveInfo(request, responder, NAMESPACE_ID.getNamespace(), APP_ID.getApplication(),
                                         ProgramType.SERVICE.getCategoryName(), PROGRAM_ID.getProgram());
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNull(exceptionThrown);
    verify(runtimeService).getLiveInfo(PROGRAM_ID);
  }
}
