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

import static org.mockito.Mockito.mock;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleManager;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.app.AppVersion;
import io.cdap.cdap.proto.app.MarkLatestAppsRequest;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.app.UpdateSourceControlMetaRequest;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * {@link ApplicationLifecycleService#markAppsAsLatest} and {@link
 * ApplicationLifecycleService#updateSourceControlMeta} are reachable from any authenticated
 * caller via {@code AppLifecycleHttpHandlerInternal}, which is registered on the same
 * general-purpose APP_FABRIC_HTTP surface as every public v3 handler (see
 * AppFabricServiceRuntimeModule). Unlike every other mutating method on this class
 * (deleteAllStates, removeApplication, deleteApplication, scanApplications), neither method
 * enforced any permission on the applications it touches, so a caller with zero access to a
 * namespace could flip which app version is treated as "latest" (the version used when a run
 * is triggered without an explicit version) or overwrite another tenant's git source-control
 * metadata.
 */
public class ApplicationLifecycleServiceMarkLatestAuthorizationTest {

  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
      Principal.PrincipalType.USER);
  private static final NamespaceId VICTIM_NAMESPACE = new NamespaceId("victim-tenant");

  private ApplicationLifecycleService service;
  private Store store;

  @Before
  public void setup() {
    // Real enforcement stack (same utility classes used by
    // FileFetcherHttpHandlerInternalAuthorizationTest / ConfigHandlerAuthorizationTest):
    // nobody has been granted anything, so any enforce() call must reject
    // UNPRIVILEGED_PRINCIPAL.
    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF),
        new Principal("master", Principal.PrincipalType.USER),
        Collections.unmodifiableSet(new HashSet<>(Collections.singletonList(
            StandardPermission.GET))));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();
    DefaultContextAccessEnforcer accessEnforcer = new DefaultContextAccessEnforcer(
        authenticationContext, inMemoryAccessController);

    store = mock(Store.class);

    CConfiguration cConf = CConfiguration.create();
    service = new ApplicationLifecycleService(cConf, store, mock(ScheduleManager.class),
        mock(UsageRegistry.class), mock(PreferencesService.class),
        mock(MetricsSystemClient.class), mock(OwnerAdmin.class),
        mock(ArtifactRepository.class),
        mock(ManagerFactory.class),
        mock(MetadataServiceClient.class), accessEnforcer, authenticationContext,
        mock(MessagingService.class), mock(Impersonator.class), mock(CapabilityReader.class),
        mock(MetricsCollectionService.class));

    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
  }

  @Test
  public void testMarkAppsAsLatestRejectsUnprivilegedCaller() throws Exception {
    MarkLatestAppsRequest request = new MarkLatestAppsRequest(
        Collections.singletonList(new AppVersion("billing-pipeline", "1.0")));

    try {
      service.markAppsAsLatest(VICTIM_NAMESPACE, request);
      Assert.fail("an unprivileged caller must not be able to mark an application as latest "
          + "in a namespace they have no access to");
    } catch (UnauthorizedException expected) {
      // expected
    }
    Mockito.verifyNoInteractions(store);
  }

  @Test
  public void testUpdateSourceControlMetaRejectsUnprivilegedCaller() throws Exception {
    UpdateMultiSourceControlMetaReqeust request = new UpdateMultiSourceControlMetaReqeust(
        Collections.singletonList(
            new UpdateSourceControlMetaRequest("billing-pipeline", "1.0", "deadbeef")),
        "commit-id");

    try {
      service.updateSourceControlMeta(VICTIM_NAMESPACE, request);
      Assert.fail("an unprivileged caller must not be able to overwrite another tenant's "
          + "source control metadata");
    } catch (UnauthorizedException expected) {
      // expected
    }
    Mockito.verifyNoInteractions(store);
  }
}
