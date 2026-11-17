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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleManager;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link ApplicationLifecycleService} exception propagation paths.
 */
public class ApplicationLifecycleServiceUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testScanApplicationsPropagatesOwnerLookupIoException() throws Exception {
    Store store = Mockito.mock(Store.class);
    OwnerAdmin ownerAdmin = Mockito.mock(OwnerAdmin.class);
    AccessEnforcer accessEnforcer = Mockito.mock(AccessEnforcer.class);
    AuthenticationContext authContext = Mockito.mock(AuthenticationContext.class);
    Principal principal = new Principal("alice", Principal.PrincipalType.USER);
    Mockito.when(authContext.getPrincipal()).thenReturn(principal);

    ApplicationId appId = NamespaceId.DEFAULT.app("app1");
    ArtifactId artifactId = new ArtifactId("art1", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "app1", "1.0.0", "6.0.0", "desc", null, artifactId,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ApplicationMeta appMeta = new ApplicationMeta("app1", appSpec, null);

    Mockito.when(store.scanApplications(
        Mockito.any(ScanApplicationsRequest.class), Mockito.anyInt(), Mockito.any()))
        .thenAnswer(invocation -> {
          BiConsumer<ApplicationId, ApplicationMeta> consumer = invocation.getArgument(2);
          consumer.accept(appId, appMeta);
          return false;
        });
    Mockito.doReturn(ImmutableSet.of(appId))
        .when(accessEnforcer).isVisible(Mockito.anySet(), Mockito.eq(principal));

    IOException ioFailure = new IOException("owner lookup failed");
    Mockito.when(ownerAdmin.getOwnerPrincipals(Mockito.anySet())).thenThrow(ioFailure);

    ApplicationLifecycleService service = createService(
        store, ownerAdmin, Mockito.mock(ArtifactRepository.class),
        Mockito.mock(ManagerFactory.class), accessEnforcer, authContext);

    try {
      service.scanApplications(NamespaceId.DEFAULT, Collections.emptyList(), detail -> { });
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException expected) {
      Assert.assertSame(ioFailure, expected.getCause());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeployAppUnwrapsExecutionExceptionCauses() throws Exception {
    ArtifactRepository artifactRepo = Mockito.mock(ArtifactRepository.class);
    ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory =
        Mockito.mock(ManagerFactory.class);
    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = Mockito.mock(Manager.class);
    Mockito.when(managerFactory.create(Mockito.any())).thenReturn(manager);

    AuthenticationContext authContext = Mockito.mock(AuthenticationContext.class);
    Mockito.when(authContext.getPrincipal())
        .thenReturn(new Principal("alice", Principal.PrincipalType.USER));

    File jarFile = TEMP_FOLDER.newFile();
    ArtifactId apiArtifactId = new ArtifactId("my-art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        NamespaceId.DEFAULT.getNamespace(), apiArtifactId, Locations.toLocation(jarFile));
    ApplicationClass appClass = new ApplicationClass("io.cdap.cdap.DummyApp", "desc", null);
    ArtifactClasses classes = ArtifactClasses.builder().addApp(appClass).build();
    ArtifactDetail detail = new ArtifactDetail(descriptor, new ArtifactMeta(classes));

    Id.Artifact legacyArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "my-art", "1.0.0");
    Mockito.when(artifactRepo.getArtifact(legacyArtifactId)).thenReturn(detail);

    ApplicationLifecycleService service = createService(
        Mockito.mock(Store.class), Mockito.mock(OwnerAdmin.class), artifactRepo,
        managerFactory, Mockito.mock(AccessEnforcer.class), authContext);

    // 1. Checked Exception cause inside ExecutionException is unwrapped directly
    ArtifactNotFoundException checkedCause =
        new ArtifactNotFoundException(NamespaceId.DEFAULT.artifact("dep", "1.0.0"));
    Mockito.when(manager.deploy(Mockito.any()))
        .thenReturn(Futures.immediateFailedFuture(checkedCause));
    try {
      service.deployApp(
          NamespaceId.DEFAULT, "app1", null, legacyArtifactId, null, null, id -> { }, null, false);
      Assert.fail("Expected ArtifactNotFoundException");
    } catch (ArtifactNotFoundException actual) {
      Assert.assertSame(checkedCause, actual);
    }

    // 2. Unchecked RuntimeException cause inside ExecutionException is unwrapped directly
    IllegalArgumentException uncheckedCause = new IllegalArgumentException("bad config");
    Mockito.when(manager.deploy(Mockito.any()))
        .thenReturn(Futures.immediateFailedFuture(uncheckedCause));
    try {
      service.deployApp(
          NamespaceId.DEFAULT, "app1", null, legacyArtifactId, null, null, id -> { }, null, false);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(uncheckedCause, actual);
    }

    // 3. Error cause inside ExecutionException is unwrapped directly
    AssertionError errorCause = new AssertionError("deploy fatal error");
    ListenableFuture<ApplicationWithPrograms> errorFuture =
        new AbstractFuture<ApplicationWithPrograms>() {
          @Override
          public ApplicationWithPrograms get() throws ExecutionException {
            throw new ExecutionException(errorCause);
          }
        };
    Mockito.when(manager.deploy(Mockito.any())).thenReturn(errorFuture);
    try {
      service.deployApp(
          NamespaceId.DEFAULT, "app1", null, legacyArtifactId, null, null, id -> { }, null, false);
      Assert.fail("Expected AssertionError");
    } catch (AssertionError actual) {
      Assert.assertSame(errorCause, actual);
    }
  }

  private static ApplicationLifecycleService createService(
      Store store,
      OwnerAdmin ownerAdmin,
      ArtifactRepository artifactRepository,
      ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authContext) {
    return new ApplicationLifecycleService(
        CConfiguration.create(),
        store,
        Mockito.mock(ScheduleManager.class),
        Mockito.mock(UsageRegistry.class),
        Mockito.mock(PreferencesService.class),
        Mockito.mock(MetricsSystemClient.class),
        ownerAdmin,
        artifactRepository,
        managerFactory,
        Mockito.mock(MetadataServiceClient.class),
        accessEnforcer,
        authContext,
        Mockito.mock(MessagingService.class),
        Mockito.mock(Impersonator.class),
        Mockito.mock(CapabilityReader.class),
        new NoOpMetricsCollectionService());
  }
}
