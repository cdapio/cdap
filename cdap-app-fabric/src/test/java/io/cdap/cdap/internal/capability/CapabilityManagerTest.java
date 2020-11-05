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
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class CapabilityManagerTest extends AppFabricTestBase {

  public static final String TEST_VERSION = "1.0.0";
  private static ApplicationLifecycleService applicationLifecycleService;
  private static CapabilityManager capabilityManager;
  private static LocationFactory locationFactory;
  private static CConfiguration cConfiguration;


  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    capabilityManager = getInjector().getInstance(CapabilityManager.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    cConfiguration = getInjector().getInstance(CConfiguration.class);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testGetAppsWithCapability() throws Exception {
    //Deploy application with capability
    Class<AppWithWorkflow> appWithWorkflowClass = AppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapabilities = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    Id.Artifact artifactIdWithCapability = deployApp(appWithWorkflowClass, appNameWithCapabilities);

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertTrue(declaredAnnotation1 == null);
    String appNameWithoutCapability = appNoCapabilityClass.getSimpleName() + UUID.randomUUID();
    deployApp(appNoCapabilityClass, appNameWithoutCapability);

    //verify that list applications return the application tagged with capability only
    for (String capability : declaredAnnotation.capabilities()) {
      EntityResult<ApplicationId> appsForCapability = capabilityManager
        .getApplications(NamespaceId.DEFAULT, capability, null, 0, 10);
      Set<ApplicationId> applicationIds = appsForCapability.getEntities().stream().collect(Collectors.toSet());
      List<ApplicationDetail> appsReturned = applicationLifecycleService.getAppDetails(applicationIds).values().stream()
        .collect(Collectors.toList());
      appsReturned.stream().
        forEach(
          applicationDetail -> Assert
            .assertEquals(appNameWithCapabilities, applicationDetail.getArtifact().getName()));
    }

    //delete the app and verify nothing is returned.
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapabilities));
    for (String capability : declaredAnnotation.capabilities()) {
      Set<ApplicationId> applicationIds = capabilityManager
        .getApplications(NamespaceId.DEFAULT, capability, null, 0, 10).getEntities().stream()
        .collect(Collectors.toSet());
      List<ApplicationDetail> appsReturned = applicationLifecycleService.getAppDetails(applicationIds).values().stream()
        .collect(Collectors.toList());
      Assert.assertTrue(appsReturned.isEmpty());
    }
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithoutCapability));
  }

  @Test
  public void testGetAppsForCapabilityPagination() throws Exception {
    //Deploy two applications with capability
    Class<AppWithWorkflow> appWithWorkflowClass = AppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapability1 = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    Id.Artifact artifactIdWithCapability1 = deployApp(appWithWorkflowClass, appNameWithCapability1);
    String appNameWithCapability2 = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    Id.Artifact artifactIdWithCapability2 = deployApp(appWithWorkflowClass, appNameWithCapability2);

    //search with offset and limit
    String capability = declaredAnnotation.capabilities()[0];
    EntityResult<ApplicationId> appsForCapability = capabilityManager
      .getApplications(NamespaceId.DEFAULT, capability, null, 0, 1);
    Assert.assertEquals(1, appsForCapability.getEntities().size());
    //next search with pagination
    EntityResult<ApplicationId> appsForCapabilityNext = capabilityManager
      .getApplications(NamespaceId.DEFAULT, capability, appsForCapability.getCursor(), 1, 1);
    Assert.assertEquals(1, appsForCapabilityNext.getEntities().size());
    EntityResult<ApplicationId> appsForCapabilityNext1 = capabilityManager
      .getApplications(NamespaceId.DEFAULT, capability, appsForCapability.getCursor(), 2, 1);
    Assert.assertEquals(0, appsForCapabilityNext1.getEntities().size());
  }

  @Test
  public void testIsApplicationDisabled() throws Exception {
    //Deploy application with capability
    Class<AppWithWorkflow> appWithWorkflowClass = AppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    String appNameWithCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    Id.Artifact artifactIdWithCapability = deployApp(appWithWorkflowClass, appNameWithCapability);

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertTrue(declaredAnnotation1 == null);
    String appNameWithOutCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployApp(appNoCapabilityClass, appNameWithOutCapability);

    boolean applicationEnabled = capabilityManager
      .isApplicationEnabled(NamespaceId.DEFAULT.getNamespace(), appNameWithCapability);
    Assert.assertEquals(false, applicationEnabled);

    //set the capabilities for the application in the enabled list
    String[] capabilities = declaredAnnotation.capabilities();
    cConfiguration.set(Constants.AppFabric.ENABLED_CAPABILITIES_LIST, String.join(",", capabilities));

    applicationEnabled = capabilityManager
      .isApplicationEnabled(NamespaceId.DEFAULT.getNamespace(), appNameWithCapability);
    Assert.assertEquals(true, applicationEnabled);

    //applications with no acclerators should not be disabled
    applicationEnabled = capabilityManager
      .isApplicationEnabled(NamespaceId.DEFAULT.getNamespace(), appNameWithOutCapability);
    Assert.assertEquals(true, applicationEnabled);

    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability));
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithOutCapability));
  }

  private Id.Artifact deployApp(Class applicationClass, String appName) throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appName, TEST_VERSION);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    //deploy app
    applicationLifecycleService
      .deployAppAndArtifact(NamespaceId.DEFAULT, appName, artifactId, appJarFile, null,
                            null, programId -> {
        }, true);
    return artifactId;
  }
}
