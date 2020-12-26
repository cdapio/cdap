/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.common.io.Files;
import io.cdap.cdap.CapabilityAppWithWorkflow;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for CapabilityApplier
 */
public class CapabilityApplierTest extends AppFabricTestBase {

  private static ApplicationLifecycleService applicationLifecycleService;
  private static ArtifactRepository artifactRepository;
  private static CapabilityApplier capabilityApplier;
  private static CapabilityWriter capabilityWriter;
  private static LocationFactory locationFactory;
  public static final String TEST_VERSION = "1.0.0";

  @BeforeClass
  public static void setup() {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    capabilityWriter = getInjector().getInstance(CapabilityWriter.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    CConfiguration cConfiguration = getInjector().getInstance(CConfiguration.class);
    DiscoveryServiceClient client = getInjector().getInstance(DiscoveryServiceClient.class);
    capabilityApplier = new CapabilityApplier(null, null, null, null, null, client);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testGetAppsWithCapability() throws Exception {
    //Deploy application with capability
    Class<CapabilityAppWithWorkflow> appWithWorkflowClass = CapabilityAppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapabilities = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    for (String capability : declaredAnnotation.capabilities()) {
      CapabilityConfig capabilityConfig = new CapabilityConfig("Enable", CapabilityStatus.ENABLED, capability,
                                                               Collections.emptyList(), Collections.emptyList());
      capabilityWriter.addOrUpdateCapability(capability, CapabilityStatus.ENABLED, capabilityConfig);
    }
    deployArtifactAndApp(appWithWorkflowClass, appNameWithCapabilities);

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertNull(declaredAnnotation1);
    String appNameWithoutCapability = appNoCapabilityClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appNoCapabilityClass, appNameWithoutCapability);

    //verify that list applications return the application tagged with capability only
    for (String capability : declaredAnnotation.capabilities()) {
      EntityResult<ApplicationId> appsForCapability = capabilityApplier
        .getApplications(NamespaceId.DEFAULT, capability, null, 0, 10);
      Set<ApplicationId> applicationIds = new HashSet<>(appsForCapability.getEntities());
      List<ApplicationDetail> appsReturned = new ArrayList<>(
        applicationLifecycleService.getAppDetails(applicationIds).values());
      appsReturned.
        forEach(
          applicationDetail -> Assert
            .assertEquals(appNameWithCapabilities, applicationDetail.getArtifact().getName()));
    }

    //delete the app and verify nothing is returned.
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapabilities, TEST_VERSION));
    for (String capability : declaredAnnotation.capabilities()) {
      Set<ApplicationId> applicationIds = new HashSet<>(capabilityApplier
                                                          .getApplications(NamespaceId.DEFAULT, capability, null, 0, 10)
                                                          .getEntities());
      List<ApplicationDetail> appsReturned = new ArrayList<>(
        applicationLifecycleService.getAppDetails(applicationIds).values());
      Assert.assertTrue(appsReturned.isEmpty());
    }
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithoutCapability, TEST_VERSION));
    artifactRepository.deleteArtifact(
      Id.Artifact.from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()), appNameWithoutCapability, TEST_VERSION));
    for (String capability : declaredAnnotation.capabilities()) {
      capabilityWriter.deleteCapability(capability);
    }
  }

  @Test
  public void testGetAppsForCapabilityPagination() throws Exception {
    //Deploy two applications with capability
    Class<CapabilityAppWithWorkflow> appWithWorkflowClass = CapabilityAppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapability1 = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    for (String capability : declaredAnnotation.capabilities()) {
      capabilityWriter.addOrUpdateCapability(capability, CapabilityStatus.ENABLED,
                                             new CapabilityConfig("Enable", CapabilityStatus.ENABLED,
                                                                  appNameWithCapability1,
                                                                  Collections
                                                                    .emptyList(), Collections.emptyList()));
    }
    deployArtifactAndApp(appWithWorkflowClass, appNameWithCapability1);
    String appNameWithCapability2 = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appWithWorkflowClass, appNameWithCapability2);

    //search with offset and limit
    String capability = declaredAnnotation.capabilities()[0];
    EntityResult<ApplicationId> appsForCapability = capabilityApplier
      .getApplications(NamespaceId.DEFAULT, capability, null, 0, 1);
    Assert.assertEquals(1, appsForCapability.getEntities().size());
    //next search with pagination
    EntityResult<ApplicationId> appsForCapabilityNext = capabilityApplier
      .getApplications(NamespaceId.DEFAULT, capability, appsForCapability.getCursor(), 1, 1);
    Assert.assertEquals(1, appsForCapabilityNext.getEntities().size());
    appsForCapabilityNext = capabilityApplier
      .getApplications(NamespaceId.DEFAULT, capability, appsForCapability.getCursor(), 2, 1);
    Assert.assertEquals(0, appsForCapabilityNext.getEntities().size());

    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability1, TEST_VERSION));
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability2, TEST_VERSION));
    artifactRepository.deleteArtifact(
      Id.Artifact.from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()), appNameWithCapability1, TEST_VERSION));
    artifactRepository.deleteArtifact(
      Id.Artifact.from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()), appNameWithCapability2, TEST_VERSION));
    for (String capability1 : declaredAnnotation.capabilities()) {
      capabilityWriter.deleteCapability(capability1);
    }
  }

  private void deployArtifactAndApp(Class<?> applicationClass, String appName) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appName, TEST_VERSION);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
    //deploy app
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appName, TEST_VERSION, artifactId,
                 null, programId -> {
        });
  }
}

