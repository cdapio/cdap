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
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.config.ConfigNotFoundException;
import io.cdap.cdap.config.ConsoleSettingsStore;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.authorization.InMemoryPrivilegeHolder;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Verifies instance {@code UPDATE} and visible-namespace authorization checks across
 * {@link OperationalStatsHttpHandler}, {@link TransactionHttpHandler},
 * {@link ConsoleSettingsHttpHandler}, {@link PreferencesHttpHandler}, and
 * {@link AppLifecycleHttpHandler}.
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
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();

    accessController = new InMemoryAccessController();
    authContext = new AuthenticationTestContext();
    contextEnforcer = new DefaultContextAccessEnforcer(authContext, accessController);

    // Bare Accessor only holds InstanceId.SELF GET (plus system namespace read bypass in CDF).
    ApplicationId systemDataprep = new ApplicationId("system", "dataprep");
    ProgramId systemDataprepSvc =
        new ProgramId(systemDataprep, ProgramType.SERVICE, "service");
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ACCESSOR,
        Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM), ACCESSOR,
        EnumSet.of(StandardPermission.GET, StandardPermission.LIST));
    accessController.grant(Authorizable.fromEntityId(systemDataprep), ACCESSOR,
        Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(systemDataprepSvc), ACCESSOR,
        Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(new NamespaceId("userns")), ACCESSOR,
        Collections.singleton(StandardPermission.GET));

    // Instance viewer on a 0-namespace instance holds NAMESPACE LIST on InstanceId.SELF.
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF, EntityType.NAMESPACE),
        VIEWER_NO_NS, Collections.singleton(StandardPermission.LIST));
    accessController.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM), VIEWER_NO_NS,
        Collections.singleton(StandardPermission.GET));

    // Instance admin holds InstanceId.SELF UPDATE and NAMESPACE LIST.
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF), ADMIN,
        EnumSet.of(StandardPermission.GET, StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(InstanceId.SELF, EntityType.NAMESPACE),
        ADMIN, Collections.singleton(StandardPermission.LIST));
  }

  @AfterClass
  public static void tearDownClass() {
    SecurityRequestContext.reset();
    InMemoryPrivilegeHolder.getPrivileges().clear();
    InMemoryPrivilegeHolder.getRoleToPrincipals().clear();
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
    Assert.assertThrows(NotFoundException.class,
        () -> handler.getServiceProviderStats(null, responder, "blah"));
  }

  @Test
  public void testTransactionsRequireInstanceUpdate() throws Exception {
    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    Mockito.when(txClient.startShort())
        .thenReturn(new Transaction(1L, 2L, new long[0], new long[0], 1L));
    Mockito.when(txClient.getSnapshotInputStream())
        .thenReturn(new ByteArrayInputStream(new byte[0]));
    TransactionHttpHandler handler = new TransactionHttpHandler(new Configuration(),
        CConfiguration.create(), txClient, contextEnforcer);
    HttpResponder responder = Mockito.mock(HttpResponder.class, Mockito.RETURNS_MOCKS);

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
    handler.getTxManagerSnapshot(null, responder);
    handler.invalidTxSize(null, responder);
    handler.invalidList(null, responder, -1);
    handler.invalidateTx(null, responder, "1");
    handler.truncateInvalidTxBefore(jsonReq("{\"time\":1}"), responder);
    handler.truncateInvalidTx(jsonReq("{\"ids\":[1]}"), responder);
    handler.resetTxManagerState(null, responder);
    Mockito.verify(txClient).getSnapshotInputStream();
    Mockito.verify(txClient).getInvalidSize();
    Mockito.verify(txClient).invalidate(1L);
    Mockito.verify(txClient).resetState();
  }

  @Test
  public void testConsoleSettingsAuthorization() throws Exception {
    ConsoleSettingsStore store = Mockito.mock(ConsoleSettingsStore.class);
    Mockito.when(store.get(Mockito.anyString())).thenThrow(new ConfigNotFoundException("", "", ""));
    NamespaceQueryAdmin emptyNsAdmin = mockNamespaceAdmin();
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
    ConsoleSettingsHttpHandler nsHandler = new ConsoleSettingsHttpHandler(
        store, contextEnforcer, mockNamespaceAdmin(NamespaceMeta.DEFAULT));
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
  public void testPreferencesInstanceAndSystemReadsRequireVisibleNamespace() throws Exception {
    PreferencesService prefService = Mockito.mock(PreferencesService.class);
    Mockito.when(prefService.getProperties()).thenReturn(Collections.emptyMap());
    Store store = Mockito.mock(Store.class);
    NamespaceStore nsStore = Mockito.mock(NamespaceStore.class);
    Mockito.when(nsStore.get(NamespaceId.SYSTEM)).thenReturn(NamespaceMeta.SYSTEM);
    Mockito.when(nsStore.get(new NamespaceId("userns")))
        .thenReturn(new NamespaceMeta.Builder().setName("userns").build());

    PreferencesHttpHandler handler = new PreferencesHttpHandler(
        accessController, authContext, prefService, store, nsStore, mockNamespaceAdmin());
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getInstancePrefs(null, responder));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getNamespacePrefs(null, responder, "system", true));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getNamespacePrefs(null, responder, "SYSTEM", false));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAppPrefs(null, responder, "system", "dataprep", true));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getProgramPrefs(null, responder, "system", "dataprep", "services", "service",
            true));
    Mockito.verifyNoInteractions(prefService);
    Mockito.verifyNoInteractions(store);

    // User-namespace preference reads still check NamespaceId GET without failing emptyNsAdmin.
    handler.getNamespacePrefs(null, responder, "userns", false);
    Mockito.verify(prefService).getProperties(new NamespaceId("userns"));

    // Instance viewer on a 0-namespace instance succeeds via NAMESPACE LIST fallback.
    AuthenticationTestContext.actAsPrincipal(VIEWER_NO_NS);
    handler.getInstancePrefs(null, responder);
    handler.getNamespacePrefs(null, responder, "system", true);
    Mockito.verify(prefService, Mockito.times(1)).getProperties();
    Mockito.verify(prefService).getResolvedProperties(NamespaceId.SYSTEM);

    // Single-namespace RBAC caller passes system read gate on instance/namespace/app/program prefs.
    PreferencesHttpHandler nsHandler = new PreferencesHttpHandler(
        accessController, authContext, prefService, store, nsStore,
        mockNamespaceAdmin(NamespaceMeta.DEFAULT));
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    nsHandler.getInstancePrefs(null, responder);
    nsHandler.getNamespacePrefs(null, responder, "system", false);
    Assert.assertThrows(ApplicationNotFoundException.class,
        () -> nsHandler.getAppPrefs(null, responder, "system", "dataprep", true));
    Assert.assertThrows(ApplicationNotFoundException.class,
        () -> nsHandler.getProgramPrefs(null, responder, "system", "dataprep", "services",
            "service", true));
    Mockito.verify(prefService, Mockito.times(2)).getProperties();
    Mockito.verify(prefService).getProperties(NamespaceId.SYSTEM);
  }

  @Test
  public void testSystemNamespaceAppReadsRequireVisibleNamespace() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    ApplicationLifecycleService appService = Mockito.mock(ApplicationLifecycleService.class);

    AppLifecycleHttpHandler handler = new AppLifecycleHttpHandler(
        cConf, Mockito.mock(ProgramRuntimeService.class), mockNamespaceAdmin(),
        Mockito.mock(NamespacePathLocator.class), appService, accessController, authContext);
    HttpResponder responder = Mockito.mock(HttpResponder.class, Mockito.RETURNS_MOCKS);

    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAppInfo(null, responder, "system", "dataprep"));
    Assert.assertThrows(UnauthorizedException.class,
        () -> handler.getAppInfo(null, responder, "SYSTEM", "dataprep"));
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

    // User-namespace reads bypass enforceSystemNamespaceReadAccess even when emptyNsAdmin is empty.
    handler.getAppInfo(null, responder, "userns", "app1");
    Mockito.verify(appService).getLatestAppDetail(new ApplicationReference("userns", "app1"));

    // Instance viewer on a 0-namespace instance passes via NAMESPACE LIST fallback.
    AuthenticationTestContext.actAsPrincipal(VIEWER_NO_NS);
    handler.getAppInfo(null, responder, "system", "dataprep");
    Mockito.verify(appService).getLatestAppDetail(new ApplicationReference("system", "dataprep"));

    // Single-namespace RBAC caller passes all 7 modified system-namespace read endpoints.
    Mockito.when(appService.getAppVersions(Mockito.any()))
        .thenReturn(Collections.singletonList("-SNAPSHOT"));
    AuthenticationTestContext.actAsPrincipal(ACCESSOR);
    AppLifecycleHttpHandler nsHandler = new AppLifecycleHttpHandler(
        cConf, Mockito.mock(ProgramRuntimeService.class), mockNamespaceAdmin(NamespaceMeta.DEFAULT),
        Mockito.mock(NamespacePathLocator.class), appService, accessController, authContext);
    nsHandler.getAppInfo(null, responder, "system", "dataprep");
    nsHandler.getAllApps(null, responder, "system", null, null, null, null, null, null,
        null, true, null, false);
    nsHandler.listAppVersions(null, responder, "system", "dataprep");
    nsHandler.getAppVersionInfo(null, responder, "system", "dataprep", "1.0.0");
    nsHandler.getPluginsInfo(null, responder, "system", "dataprep");
    nsHandler.getAppsCount(null, responder, "system");
    nsHandler.getApplicationDetails(jsonReq("[{\"appId\":\"dataprep\"}]"), responder, "system");
    Mockito.verify(appService).scanApplications(Mockito.any(), Mockito.any());
    Mockito.verify(appService).getAppVersions(Mockito.any());
    Mockito.verify(appService).getAppDetail(new ApplicationId("system", "dataprep", "1.0.0"));
    Mockito.verify(appService).getPlugins(Mockito.any());
    Mockito.verify(appService).getApplicationsCount(NamespaceId.SYSTEM);
    Mockito.verify(appService).getAppDetails(Mockito.any());
  }

  private static NamespaceQueryAdmin mockNamespaceAdmin(NamespaceMeta... namespaces)
      throws Exception {
    NamespaceQueryAdmin nsAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(nsAdmin.list()).thenReturn(Arrays.asList(namespaces));
    return nsAdmin;
  }

  private static FullHttpRequest jsonReq(String body) {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3/test",
        Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
  }
}
