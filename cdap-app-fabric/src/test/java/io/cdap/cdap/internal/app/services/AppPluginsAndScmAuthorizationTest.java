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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.operation.OperationLifecycleManager;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.NamespacePermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.PullMultipleAppsRequest;
import io.cdap.cdap.proto.sourcecontrol.PushMultipleAppsRequest;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.time.Clock;
import java.util.EnumSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Authorization tests for {@link ApplicationLifecycleService#getPlugins} and the multi-app push /
 * pull methods of {@link SourceControlManagementService}.
 */
public class AppPluginsAndScmAuthorizationTest {

  private static final Principal ALICE = new Principal("apsa-alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("apsa-bob", Principal.PrincipalType.USER);
  private static final Principal CAROL = new Principal("apsa-carol", Principal.PrincipalType.USER);

  private static final NamespaceId NAMESPACE = new NamespaceId("apsans");
  private static final String APP1 = "app1";
  private static final String APP2 = "app2";

  private static InMemoryAccessController accessController;
  private static AuthenticationContext authenticationContext;

  @BeforeClass
  public static void setup() {
    accessController = new InMemoryAccessController();
    authenticationContext = new AuthenticationTestContext();
    for (Principal principal : ImmutableList.of(ALICE, BOB, CAROL)) {
      grant(NAMESPACE, principal,
          EnumSet.of(NamespacePermission.READ_REPOSITORY, NamespacePermission.WRITE_REPOSITORY));
    }
    // ALICE can view and create both apps, BOB can only view both apps, CAROL can only view app1
    grant(NAMESPACE.app(APP1), ALICE, EnumSet.of(StandardPermission.GET, StandardPermission.CREATE));
    grant(NAMESPACE.app(APP2), ALICE, EnumSet.of(StandardPermission.GET, StandardPermission.CREATE));
    grant(NAMESPACE.app(APP1), BOB, EnumSet.of(StandardPermission.GET));
    grant(NAMESPACE.app(APP2), BOB, EnumSet.of(StandardPermission.GET));
    grant(NAMESPACE.app(APP1), CAROL, EnumSet.of(StandardPermission.GET));
  }

  @Test
  public void testGetPlugins() throws Exception {
    Store store = Mockito.mock(Store.class);
    ApplicationReference appRef = NAMESPACE.appReference(APP2);
    Mockito.when(store.getLatest(appRef)).thenReturn(
        new ApplicationMeta(APP2, Mockito.mock(ApplicationSpecification.class), null));
    ApplicationLifecycleService service = new ApplicationLifecycleService(
        CConfiguration.create(), store, null, null, null, null, null, null, null, null,
        accessController, authenticationContext, null, null, null,
        new NoOpMetricsCollectionService());

    AuthenticationTestContext.actAsPrincipal(CAROL);
    try {
      service.getPlugins(appRef);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Mockito.verifyNoInteractions(store);

    AuthenticationTestContext.actAsPrincipal(BOB);
    Assert.assertTrue(service.getPlugins(appRef).isEmpty());
  }

  @Test
  public void testPushAndPullApps() throws Exception {
    OperationLifecycleManager operationManager = Mockito.mock(OperationLifecycleManager.class);
    ApplicationLifecycleService appLifecycleService =
        Mockito.mock(ApplicationLifecycleService.class);
    Mockito.when(appLifecycleService.decodeUserId(Mockito.any())).thenReturn("user");
    SourceControlManagementService service = Mockito.spy(new SourceControlManagementService(
        CConfiguration.create(), Mockito.mock(SecureStore.class),
        Mockito.mock(TransactionRunner.class), accessController, authenticationContext,
        Mockito.mock(SourceControlOperationRunner.class), appLifecycleService,
        Mockito.mock(Store.class), operationManager, new NoOpMetricsCollectionService(),
        Clock.systemUTC()));
    Mockito.doReturn(Mockito.mock(RepositoryMeta.class)).when(service).getRepositoryMeta(NAMESPACE);
    PushMultipleAppsRequest pushRequest =
        new PushMultipleAppsRequest(ImmutableList.of(APP1, APP2), "commit");
    PullMultipleAppsRequest pullRequest = new PullMultipleAppsRequest(ImmutableList.of(APP1, APP2));

    // push requires GET on every app, checked before the operation is created
    AuthenticationTestContext.actAsPrincipal(CAROL);
    try {
      service.pushApps(NAMESPACE, pushRequest);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    AuthenticationTestContext.actAsPrincipal(BOB);
    service.pushApps(NAMESPACE, pushRequest);
    Mockito.verify(operationManager).createPushOperation(Mockito.eq(NAMESPACE.getNamespace()),
        Mockito.any(), Mockito.any(), Mockito.any());

    // pull requires CREATE on every app, checked before the operation is created
    try {
      service.pullApps(NAMESPACE, pullRequest);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Mockito.verify(operationManager, Mockito.never()).createPullOperation(Mockito.any(),
        Mockito.any(), Mockito.any(), Mockito.any());
    AuthenticationTestContext.actAsPrincipal(ALICE);
    service.pullApps(NAMESPACE, pullRequest);
    Mockito.verify(operationManager).createPullOperation(Mockito.eq(NAMESPACE.getNamespace()),
        Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verifyNoMoreInteractions(operationManager);
  }

  private static void grant(EntityId entity, Principal principal,
      Set<? extends Permission> permissions) {
    accessController.grant(Authorizable.fromEntityId(entity), principal, permissions);
  }
}
