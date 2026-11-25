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

import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.config.ConfigNotFoundException;
import io.cdap.cdap.config.ConsoleSettingsStore;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.store.NamespaceStore;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Verifies Option 1 RBAC checks across {@link OperationalStatsHttpHandler},
 * {@link TransactionHttpHandler}, {@link ConsoleSettingsHttpHandler},
 * {@link PreferencesHttpHandler}, and {@link AppLifecycleHttpHandler}.
 */
public class AccessorHardeningAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Principal ACCESSOR = new Principal("accessor", Principal.PrincipalType.USER);
  private static final Principal VIEWER_NO_NS =
      new Principal("viewer-no-ns", Principal.PrincipalType.USER);
  private static final Principal ADMIN = new Principal("admin", Principal.PrincipalType.USER);

  private static InMemoryAccessController accessController;
  private static AuthenticationContext authContext;
  private static ContextAccessEnforcer contextEnforcer;

  @BeforeClass
  public static void setUpClass() {
    accessController = new InMemoryAccessController();
    authContext = new AuthenticationTestContext();
    contextEnforcer = new DefaultContextAccessEnforcer(authContext, accessController);

    // Bare Accessor only holds InstanceId.SELF GET (plus system namespace read bypass in CDF).
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ACCESSOR,
        Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM), ACCESSOR,
        EnumSet.of(StandardPermission.GET, StandardPermission.LIST));
    accessController.grant(
        Authorizable.fromEntityId(new ApplicationId("system", "dataprep")), ACCESSOR,
        Collections.singleton(StandardPermission.GET));

    // Instance viewer on a 0-namespace instance holds NAMESPACE LIST on InstanceId.SELF.
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF, EntityType.NAMESPACE),
        VIEWER_NO_NS, Collections.singleton(StandardPermission.LIST));

    // Instance admin holds InstanceId.SELF UPDATE and NAMESPACE LIST.
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ADMIN,
        EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF, EntityType.NAMESPACE),
        ADMIN, Collections.singleton(StandardPermission.LIST));
  }

  @Test
  public void testOperationalStatsRequiresInstanceUpdate() throws Exception {
    OperationalStatsHttpHandler handler = new OperationalStatsHttpHandler(contextEnforcer);
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getServiceProviders(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getServiceProviderStats(null, responder, "blah"));
    Mockito.verifyNoInteractions(responder);

    AuthenticationTestContext.actAsPrincipal(ADMIN);
    handler.getServiceProviders(null, responder);
  }

  @Test
  public void testTransactionsRequireInstanceUpdate() throws Exception {
    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    Mockito.when(txClient.startShort())
        .thenReturn(new Transaction(1L, 2L, new long[0], new long[0], 1L));
    Mockito.when(txClient.getSnapshotInputStream())
        .thenReturn(new ByteArrayInputStream(new byte[0]));
    TransactionHttpHandler handler = new TransactionHttpHandler(new Configuration(),
        CConfiguration.create(), txClient, accessController, authContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getTxManagerSnapshot(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.invalidTxSize(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.invalidList(null, responder, -1));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getPruneInfo(null, responder, "r1"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getTimeRegions(null, responder, "now"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getIdleRegions(null, responder, -1, "now"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getRegionsToBeCompacted(null, responder, -1, "now"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.invalidateTx(null, responder, "1"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.truncateInvalidTxBefore(jsonReq("{\"time\":1}"), responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.truncateInvalidTx(jsonReq("{\"ids\":[1]}"), responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.resetTxManagerState(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.pruneNow(null, responder));
    Mockito.verifyNoInteractions(txClient);

    AuthenticationTestContext.actAsPrincipal(ADMIN);
    handler.invalidTxSize(null, responder);
    Mockito.verify(txClient).getInvalidSize();
  }

  @Test
  public void testConsoleSettingsAuthorization() throws Exception {
    ConsoleSettingsStore store = Mockito.mock(ConsoleSettingsStore.class);
    Mockito.when(store.get(Mockito.anyString())).thenThrow(new ConfigNotFoundException("", "", ""));
    NamespaceQueryAdmin emptyNsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(emptyNsAdmin.list()).thenReturn(Collections.emptyList());
    ConsoleSettingsHttpHandler handler = new ConsoleSettingsHttpHandler(
        store, contextEnforcer, emptyNsAdmin);
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class, () -> handler.get(null, responder));
    Assert.assertThrows(UnauthorizedException.class, () -> handler.delete(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.set(jsonReq("{\"a\":1}"), responder));
    Mockito.verifyNoInteractions(store);

    // Single-namespace RBAC user (visible namespace) can GET, but still cannot PUT or DELETE.
    NamespaceQueryAdmin singleNsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(singleNsAdmin.list()).thenReturn(Collections.singletonList(NamespaceMeta.DEFAULT));
    ConsoleSettingsHttpHandler nsHandler = new ConsoleSettingsHttpHandler(
        store, contextEnforcer, singleNsAdmin);
    nsHandler.get(null, responder);
    Assert.assertThrows(UnauthorizedException.class, () -> nsHandler.delete(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> nsHandler.set(jsonReq("{\"a\":1}"), responder));

    // Instance viewer on a 0-namespace instance can GET via NAMESPACE LIST fallback.
    AuthenticationTestContext.actAsPrincipal(VIEWER_NO_NS);
    handler.get(null, responder);

    // Admin can PUT and DELETE.
    AuthenticationTestContext.actAsPrincipal(ADMIN);
    handler.set(jsonReq("{\"a\":1}"), responder);
    handler.delete(null, responder);
    Mockito.verify(store).put(Mockito.any());
    Mockito.verify(store).delete("");
  }

  @Test
  public void testPreferencesGetInstanceRequiresVisibleNamespace() throws Exception {
    PreferencesService prefService = Mockito.mock(PreferencesService.class);
    Mockito.when(prefService.getProperties()).thenReturn(Collections.emptyMap());
    NamespaceQueryAdmin emptyNsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(emptyNsAdmin.list()).thenReturn(Collections.emptyList());
    PreferencesHttpHandler handler = new PreferencesHttpHandler(
        accessController, authContext, prefService,
        Mockito.mock(Store.class), Mockito.mock(NamespaceStore.class), emptyNsAdmin);
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getInstancePrefs(null, responder));
    Mockito.verifyNoInteractions(prefService);

    AuthenticationTestContext.actAsPrincipal(VIEWER_NO_NS);
    handler.getInstancePrefs(null, responder);
    Mockito.verify(prefService, Mockito.times(1)).getProperties();

    NamespaceQueryAdmin singleNsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(singleNsAdmin.list()).thenReturn(Collections.singletonList(NamespaceMeta.DEFAULT));
    PreferencesHttpHandler nsHandler = new PreferencesHttpHandler(
        accessController, authContext, prefService,
        Mockito.mock(Store.class), Mockito.mock(NamespaceStore.class), singleNsAdmin);
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    nsHandler.getInstancePrefs(null, responder);
    Mockito.verify(prefService, Mockito.times(2)).getProperties();
  }

  @Test
  public void testSystemNamespaceAppReadsRequireVisibleNamespace() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set("data.local.dir", TEMP_FOLDER.newFolder().getAbsolutePath());
    ApplicationLifecycleService appService = Mockito.mock(ApplicationLifecycleService.class);
    NamespaceQueryAdmin emptyNsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(emptyNsAdmin.list()).thenReturn(Collections.emptyList());

    AppLifecycleHttpHandler handler = new AppLifecycleHttpHandler(
        cConf, Mockito.mock(ProgramRuntimeService.class), emptyNsAdmin,
        Mockito.mock(NamespacePathLocator.class), appService, accessController, authContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class, Mockito.RETURNS_MOCKS);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAppInfo(null, responder, "system", "dataprep"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAllApps(null, responder, "system", null, null, null, null, null, null,
            null, true, null, false));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.listAppVersions(null, responder, "system", "dataprep"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAppVersionInfo(null, responder, "system", "dataprep", "-SNAPSHOT"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getPluginsInfo(null, responder, "system", "dataprep"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAppsCount(null, responder, "system"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getApplicationDetails(jsonReq("[{\"appId\":\"dataprep\"}]"), responder,
            "system"));
    Mockito.verifyNoInteractions(appService);

    // Caller with at least one visible namespace passes the gate and reaches ApplicationLifecycleService.
    NamespaceQueryAdmin singleNsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(singleNsAdmin.list()).thenReturn(Collections.singletonList(NamespaceMeta.DEFAULT));
    AppLifecycleHttpHandler nsHandler = new AppLifecycleHttpHandler(
        cConf, Mockito.mock(ProgramRuntimeService.class), singleNsAdmin,
        Mockito.mock(NamespacePathLocator.class), appService, accessController, authContext);
    nsHandler.getAppInfo(null, responder, "system", "dataprep");
    Mockito.verify(appService).getLatestAppDetail(Mockito.any());
  }

  private static FullHttpRequest jsonReq(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3/test",
        Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }
}
