/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithProgramsUsingGuava;
import io.cdap.cdap.CapabilityAppWithWorkflow;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.MetadataEmitApp;
import io.cdap.cdap.MissingMapReduceWorkflowApp;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ProgramResources;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.system.AppSystemMetadataWriter;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.capability.CapabilityConfig;
import io.cdap.cdap.internal.capability.CapabilityNotAvailableException;
import io.cdap.cdap.internal.capability.CapabilityStatus;
import io.cdap.cdap.internal.capability.CapabilityWriter;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.app.AppVersion;
import io.cdap.cdap.proto.app.MarkLatestAppsRequest;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.app.UpdateSourceControlMetaRequest;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class ApplicationLifecycleServiceTest extends AppFabricTestBase {
  private static final String FEATURE_FLAG_PREFIX = "feature.";
  private static ApplicationLifecycleService applicationLifecycleService;
  private static LocationFactory locationFactory;
  private static ArtifactRepository artifactRepository;
  private static MetadataStorage metadataStorage;
  private static CapabilityWriter capabilityWriter;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    metadataStorage = getInjector().getInstance(MetadataStorage.class);
    capabilityWriter = getInjector().getInstance(CapabilityWriter.class);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  private void setLcmEditFlag(boolean lcmFlag) {
    cConf.setBoolean(
        FEATURE_FLAG_PREFIX + Feature.LIFECYCLE_MANAGEMENT_EDIT.getFeatureFlagString(), lcmFlag);
  }

  @Before
  public void setDefaultFeatureFlags() {
    setLcmEditFlag(true);
  }

  // test that the call to deploy an artifact and application in a single step will delete the artifact
  // if the application could not be created
  @Test(expected = ArtifactNotFoundException.class)
  public void testDeployArtifactAndApplicationCleansUpArtifactOnFailure() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "missing-mr", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, MissingMapReduceWorkflowApp.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopyOverwrite(appJar, appJarFile);
    appJar.delete();

    try {
      applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName", artifactId, appJarFile, null,
                                                       null, programId -> {
        }, true);
      Assert.fail("expected application deployment to fail.");
    } catch (Exception e) {
      // expected
    }

    // the artifact should have been cleaned up, and this should throw a not found exception
    artifactRepository.getArtifact(artifactId);
  }

  @Test
  public void testAppDeletionMultipleNS() throws Exception {

    // deploy same app in two namespaces
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail applicationDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    ProgramId serviceId = new ProgramId(TEST_NAMESPACE1, AllProgramsApp.NAME, applicationDetail.getAppVersion(),
                                        ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    ApplicationId testNSAppId = new ApplicationId(TEST_NAMESPACE1, AllProgramsApp.NAME);
    testNSAppId = new ApplicationId(testNSAppId.getNamespace(), testNSAppId.getApplication(),
                                    applicationDetail.getAppVersion());
    applicationDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);
    ApplicationId defaultNSAppId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME,
                                       applicationDetail.getAppVersion());

    // start a program in one namespace
    // service is stopped initially
    Assert.assertEquals("STOPPED", getProgramStatus(Id.Program.fromEntityId(serviceId)));
    // start the service and check the status
    startProgram(Id.Program.fromEntityId(serviceId));
    waitState(serviceId, ProgramRunStatus.RUNNING.toString());

    // delete the app from another (default namespace)
    deleteAppAndData(defaultNSAppId);

    // cleanup
    stopProgram(Id.Program.fromEntityId(serviceId));
    waitState(serviceId, "STOPPED");
    deleteAppAndData(testNSAppId);
  }

  @Test
  public void testCapabilityMetaDataDeletion() throws Exception {
    Class<CapabilityAppWithWorkflow> appWithWorkflowClass = CapabilityAppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    Set<String> expected = Arrays.stream(declaredAnnotation.capabilities()).collect(Collectors.toSet());
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appWithWorkflowClass.getSimpleName(), "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appWithWorkflowClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopyOverwrite(appJar, appJarFile);
    appJar.delete();

    //deploy app
    try {
      applicationLifecycleService
        .deployAppAndArtifact(NamespaceId.DEFAULT, appWithWorkflowClass.getSimpleName(), artifactId, appJarFile, null,
                              null, programId -> {
          }, true);
      Assert.fail("Expecting exception");
    } catch (CapabilityNotAvailableException ex) {
      //expected
    }
    for (String capability : declaredAnnotation.capabilities()) {
      CapabilityConfig capabilityConfig = new CapabilityConfig("Test", CapabilityStatus.ENABLED, capability,
                                                               Collections.emptyList(), Collections.emptyList(),
                                                               Collections.emptyList());
      capabilityWriter.addOrUpdateCapability(capability, CapabilityStatus.ENABLED, capabilityConfig);
    }
    applicationLifecycleService
      .deployAppAndArtifact(NamespaceId.DEFAULT, appWithWorkflowClass.getSimpleName(), artifactId, appJarFile, null,
                            null, programId -> {
        }, true);
    // Check for the capability metadata
    ApplicationId appId = NamespaceId.DEFAULT.app(appWithWorkflowClass.getSimpleName());
    MetadataEntity appMetadataId = appId.toMetadataEntity();
    Assert.assertFalse(metadataStorage.read(new Read(appMetadataId,
                                                     MetadataScope.SYSTEM, MetadataKind.PROPERTY)).isEmpty());
    Map<String, String> metadataProperties =
      metadataStorage.read(new Read(appMetadataId)).getProperties(MetadataScope.SYSTEM);
    String capabilityMetaData = metadataProperties.get(AppSystemMetadataWriter.CAPABILITY_TAG);
    Set<String> actual = Arrays.stream(capabilityMetaData.split(AppSystemMetadataWriter.CAPABILITY_DELIMITER))
                           .collect(Collectors.toSet());
    Assert.assertEquals(expected, actual);

    // Remove the application and verify that all metadata is removed
    ApplicationDetail applicationDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(),
                                                        appWithWorkflowClass.getSimpleName());
    appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appWithWorkflowClass.getSimpleName(),
                              applicationDetail.getAppVersion());
    applicationLifecycleService.removeApplication(appId.getAppReference());
  }

  @Test
  public void testMetadataEmitInConfigure() throws Exception {
    deploy(MetadataEmitApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           NamespaceId.DEFAULT.getNamespace());

    ApplicationDetail applicationDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(), MetadataEmitApp.NAME);
    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), MetadataEmitApp.NAME,
                              applicationDetail.getAppVersion());

    // check app user metadata gets emitted correctly
    Metadata userMetadata = metadataStorage.read(new Read(appId.toMetadataEntity(), MetadataScope.USER));
    Assert.assertEquals(MetadataEmitApp.USER_METADATA.getProperties(), userMetadata.getProperties(MetadataScope.USER));
    Assert.assertEquals(MetadataEmitApp.USER_METADATA.getTags(), userMetadata.getTags(MetadataScope.USER));

    Metadata systemMetadata = metadataStorage.read(new Read(appId.toMetadataEntity(), MetadataScope.SYSTEM));
    // here system properties will contain what emitted in the app + the ones emitted by the platform,
    // we only compare the ones emitted by the app
    Map<String, String> sysProperties = systemMetadata.getProperties(MetadataScope.SYSTEM);
    MetadataEmitApp.SYS_METADATA.getProperties().forEach((key, val) -> {
      Assert.assertEquals(val, sysProperties.get(key));
    });
    // check the tags contain all the tags emitted by the app
    Assert.assertTrue(systemMetadata.getTags(MetadataScope.SYSTEM).containsAll(MetadataEmitApp.SYS_METADATA.getTags()));
    applicationLifecycleService.removeApplication(appId.getAppReference());
  }

  /**
   * Some tests for owner information storage/propagation during app deployment.
   * More tests at handler level {@link io.cdap.cdap.internal.app.services.http.handlers.AppLifecycleHttpHandlerTest}
   */
  @Test
  public void testOwner() throws Exception {
    String ownerPrincipal = "alice/somehost.net@somekdc.net";
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE2, ownerPrincipal);

    // trying to redeploy the same app as a different owner should fail
    deploy(AllProgramsApp.class, HttpResponseStatus.FORBIDDEN.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE2, "bob/somehost.net@somekdc.net");

    // although trying to re-deploy the app with same owner should work
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
                                 TEST_NAMESPACE2, ownerPrincipal);
    ApplicationDetail applicationDetail = getAppDetails(TEST_NAMESPACE2, AllProgramsApp.NAME);

    // delete, otherwise it conflicts with other tests
    deleteAppAndData(new ApplicationId(TEST_NAMESPACE2, AllProgramsApp.NAME,
                                       applicationDetail.getAppVersion()));
  }

  @Test
  public void testOwnerInImpersonatedNS() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";
    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName("impNs").setPrincipal(nsPrincipal).setKeytabUri(nsKeytabURI).build();
    createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());

    // deploy an app without owner
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName());

    // try to redeploy with some owner different than namespace principal it should fail
    deploy(AllProgramsApp.class, HttpResponseStatus.FORBIDDEN.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), "bob/somehost.net@somekdc.net");

    // although trying to re-deploy the app with namespace principal should work
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), nsPrincipal);

    // re-deploy without any owner should also work
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName());

    // cleanup
    deleteNamespace(impNsMeta.getName());
    Assert.assertEquals(HttpResponseCodes.SC_NOT_FOUND,
                        getNamespace(impNsMeta.getName()).getResponseCode());

    // create an impersonated ns again
    createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());

    // deploy an app with an owner
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), "bob/somehost.net@somekdc.net");

    // re-deploy with namespace principal should fail
    deploy(AllProgramsApp.class, HttpResponseStatus.FORBIDDEN.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), impNsMeta.getConfig().getPrincipal());

    // although redeploy with same app principal should work
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), "bob/somehost.net@somekdc.net");

    ApplicationDetail applicationDetail = getAppDetails(impNsMeta.getNamespaceId().getNamespace(), AllProgramsApp.NAME);

    // delete, otherwise it conflicts with other tests
    deleteAppAndData(new ApplicationId(impNsMeta.getNamespaceId().getNamespace(), AllProgramsApp.NAME,
                                       applicationDetail.getAppVersion()));
  }

  @Test
  public void testMissingDependency() throws Exception {
    // tests the fix for CDAP-2543, by having programs which fail to start up due to missing dependency jars
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("missing-guava-dependency", "1.0.0-SNAPSHOT");
    Location appJar = createDeploymentJar(locationFactory, AppWithProgramsUsingGuava.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Locations.linkOrCopyOverwrite(appJar, appJarFile);
    appJar.delete();

    applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName",
                                                     Id.Artifact.fromEntityId(artifactId), appJarFile,
                                                     null, null, new ProgramTerminator() {
        @Override
        public void stop(ProgramId programId) throws Exception {
          // no-op
        }
      }, true);

    ApplicationId appId = NamespaceId.DEFAULT.app("appName");

    // run records for programs that have missing dependencies should be FAILED, instead of hanging in RUNNING

    // fail the Worker#initialize
    ProgramId worker = appId.worker(AppWithProgramsUsingGuava.NoOpWorker.NAME);
    startProgram(Id.Program.fromEntityId(worker));
    waitForRuns(1, worker, ProgramRunStatus.FAILED);

    // fail the MapReduce#initialize
    ProgramId mapreduce = appId.mr(AppWithProgramsUsingGuava.NoOpMR.NAME);
    startProgram(Id.Program.fromEntityId(mapreduce));
    waitForRuns(1, mapreduce, ProgramRunStatus.FAILED);

    // fail the CustomAction#initialize
    ProgramId workflow = appId.workflow(AppWithProgramsUsingGuava.NoOpWorkflow.NAME);
    startProgram(Id.Program.fromEntityId(workflow));
    waitForRuns(1, workflow, ProgramRunStatus.FAILED);

    // fail the Workflow#initialize
    appId.workflow(AppWithProgramsUsingGuava.NoOpWorkflow.NAME);
    startProgram(Id.Program.fromEntityId(workflow), ImmutableMap.of("fail.in.workflow.initialize", "true"));
    waitForRuns(1, workflow, ProgramRunStatus.FAILED);
  }

  @Test
  public void testScanApplications() throws Exception {
    createNamespace("ns1");
    createNamespace("ns2");
    createNamespace("ns3");

    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns1");
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns2");
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns3");

    List<ApplicationDetail> appDetails = new ArrayList<>();

    applicationLifecycleService.scanApplications(new NamespaceId("ns1"), ImmutableSet.of(), null,
        d -> appDetails.add(d));

    Assert.assertEquals(appDetails.size(), 1);
    deleteNamespace("ns1");
    deleteNamespace("ns2");
    deleteNamespace("ns3");
  }

  @Test
  public void testScanApplicationsWithFailingPredicate() throws Exception {
    createNamespace("ns1");
    createNamespace("ns2");
    createNamespace("ns3");

    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns1");
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns2");
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns3");

    List<ApplicationDetail> appDetails = new ArrayList<>();

    applicationLifecycleService.scanApplications(new NamespaceId("ns1"), ImmutableSet.of("name1"), "version1",
        d -> appDetails.add(d));

    Assert.assertEquals(appDetails.size(), 0);
    deleteNamespace("ns1");
    deleteNamespace("ns2");
    deleteNamespace("ns3");
  }

  @Test
  public void testCreateAppDetailsArchive() throws Exception {
    createNamespace("ns1");
    createNamespace("ns2");
    createNamespace("ns3");

    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns1");
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns2");
    deploy(AllProgramsApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN, "ns3");

    File archiveFile = tmpFolder.newFile();
    try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(archiveFile))) {
      applicationLifecycleService.createAppDetailsArchive(zipOut);
    }

    // Validate the ZIP file content
    File dir = tmpFolder.newFolder();
    BundleJarUtil.unJar(archiveFile, dir);

    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    for (String ns : Arrays.asList("ns1", "ns2", "ns3")) {
      File nsDir = new File(dir, ns);
      Assert.assertTrue(nsDir.isDirectory());

      ApplicationDetail appDetail = GSON.fromJson(Files.toString(new File(nsDir, appSpec.getName() + ".json"),
                                                                 StandardCharsets.UTF_8),
                                                  ApplicationDetail.class);
      Assert.assertEquals(appSpec.getName(), appDetail.getName());

      // Check if all the programs are there
      int programCount = Arrays.stream(io.cdap.cdap.api.app.ProgramType.values())
        .map(appSpec::getProgramsByType)
        .mapToInt(Set::size)
        .reduce(0, Integer::sum);
      Assert.assertEquals(programCount, appDetail.getPrograms().size());

      for (ProgramRecord record : appDetail.getPrograms()) {
        Assert.assertTrue(appSpec.getProgramsByType(record.getType().getApiProgramType()).contains(record.getName()));
      }
    }
    deleteNamespace("ns1");
    deleteNamespace("ns2");
    deleteNamespace("ns3");
  }

  /**
   * Testcase to deploy an application without marking it as latest. (using the
   * internal API to deploy, with the skipMarkingLatest flag set to true).
   * The appliaction should be deployed successfully.
   * There should be only one version of the application deployed, and that should not be marked latest.
   * Therefore, trying to get the latest version of the application should throw ApplicationNotFoundException.
   *
   * @throws Exception it's expected to throw {@link ApplicationNotFoundException}
   */
  @Test(expected = ApplicationNotFoundException.class)
  public void testDeployWithoutMarkingAppAsLatest() throws Exception {
    String appName = "application_not_marked_latest";
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appName);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);
    HttpResponse resp = deployWithoutMarkingLatest(
        appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));
    // the application deployment should succeed
    Assert.assertEquals(200, resp.getResponseCode());
    List<String> deployedAppVersions = applicationLifecycleService.getAppVersions(
        appId.toEntityId().getAppReference()
    ).stream().collect(Collectors.toList());

    // there should be only one version of this application deployed
    Assert.assertEquals(1, deployedAppVersions.size());
    String deployedVersion = deployedAppVersions.get(0);
    // the deployed version should be accessible in the applicationLifecycleService by ApplicationId
    ApplicationDetail appDetail = applicationLifecycleService.getAppDetail(new ApplicationId(
        Namespace.DEFAULT.getId(), appName, deployedVersion
    ));
    Assert.assertEquals(appName, appDetail.getName());
    Assert.assertEquals(deployedVersion, appDetail.getAppVersion());

    // But as the only deployed version is not marked as latest, trying to get the
    // latest application by ApplicationReference should throw ApplicationNotFoundException (expected).
    applicationLifecycleService.getLatestAppDetail(appId.toEntityId().getAppReference());
  }

  /**
   * Testcase to deploy an application without marking it as latest (using the
   * internal API to deploy, with the skipMarkingLatest flag set to true).
   * And then mark the deployed application as latest using the markAppsAsLatest method.
   *
   * @throws Exception when the test crashes (not expected)
   */
  @Test
  public void testMarkAppsAsLatest() throws Exception {
    String appName = "application_to_be_marked_latest";
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appName);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);
    HttpResponse resp = deployWithoutMarkingLatest(
        appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));

    // the application deployment should succeed
    Assert.assertEquals(200, resp.getResponseCode());
    List<String> deployedAppVersions = applicationLifecycleService.getAppVersions(
        appId.toEntityId().getAppReference()
    ).stream().collect(Collectors.toList());
    // there should be only one version of this application deployed
    Assert.assertEquals(1, deployedAppVersions.size());
    String deployedVersion = deployedAppVersions.get(0);

    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Collections.singletonList(new AppVersion(appName, deployedVersion))));

    // now that the deployed application has been marked latest, trying to get the
    // latest version of the application by ApplicationReference should succeed. And
    // it should return the version of the application that we marked latest in the previous step.
    ApplicationDetail appDetail = applicationLifecycleService.getLatestAppDetail(appId.toEntityId().getAppReference());
    Assert.assertEquals(appName, appDetail.getName());
    Assert.assertEquals(deployedVersion, appDetail.getAppVersion());
  }

  /**
   * Testcase to verify that the markAppsAsLatest method throws BadRequestException when
   * we try to mark multiple versions of the same application as latest.
   *
   * @throws Exception this test is expected to throw {@link BadRequestException}
   */
  @Test(expected = BadRequestException.class)
  public void testMarkAppsAsLatestWithDuplicateAppIds() throws Exception {
    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Arrays.asList(new AppVersion("app1", "v1"),
                new AppVersion("app1", "v2"))
        ));
  }

  /**
   * Testcase to verify that the markAppsAsLatest method throws BadRequestException when
   * we provide any Invalid application id.
   *
   * @throws Exception this test is expected to throw {@link BadRequestException}
   */
  @Test(expected = BadRequestException.class)
  public void testMarkAppsAsLatestWithInvalidAppId() throws Exception {
    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Collections.singletonList(new AppVersion("invalid app id", "v1"))
        ));
  }

  /**
   * Testcase to verify that the markAppsAsLatest method throws BadRequestException when
   * any of the given application id is null.
   *
   * @throws Exception this test is expected to throw {@link BadRequestException}
   */
  @Test(expected = BadRequestException.class)
  public void testMarkAppsAsLatestWithNullAppId() throws Exception {
    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Collections.singletonList(new AppVersion(null, "v1"))
        ));
  }

  /**
   * Testcase to verify that the markAppsAsLatest method throws BadRequestException when
   * any of the given application version is null.
   *
   * @throws Exception this test is expected to throw {@link BadRequestException}
   */
  @Test(expected = BadRequestException.class)
  public void testMarkAppsAsLatestWithNullAppVersion() throws Exception {
    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Collections.singletonList(new AppVersion("app", null))
        ));
  }

  /**
   * Testcase to verify that the markAppsAsLatest method throws ApplicationNotFoundException when
   * any of the given application does not exist (or not found).
   * In this test, we deploy an application without marking it as latest.
   * Then we try to mark 2 applications as latest, one of them being the one that we deployed,
   * and the other being an application id that does not exist. In this case, as one of the
   * applications was not found, the markAppsAsLatest method should fail with the proper exception.
   *
   * @throws Exception this test is expected to throw {@link ApplicationNotFoundException}
   */
  @Test(expected = ApplicationNotFoundException.class)
  public void testMarkAppsAsLatestWithNonExistingAppVersion() throws Exception {
    String appName = "existing_app";
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appName);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);
    HttpResponse resp = deployWithoutMarkingLatest(
        appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));
    Assert.assertEquals(200, resp.getResponseCode());
    String deployedVersion = applicationLifecycleService.getAppVersions(
        appId.toEntityId().getAppReference()
    ).stream().collect(Collectors.toList()).get(0);


    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Arrays.asList(new AppVersion("non_existing_app", "v1"),
                new AppVersion(appName, deployedVersion))
        ));
  }

  /**
   * Testcase to verify the intended behaviour of the markAppsAsLatest method.
   * In this case, we deploy two versions of the same application one after another.
   * The first version will be deployed using the public deploy API (without the skipMarkingLatest flag).
   * Therefore the first verison should be marked as latest initially.
   * Then the second version will be deployed using the internal deploy API (with skipMarkingLatest=true).
   * At this point,  we should get the first version should be returned when we try to get the latest version.
   * Then, using the markAppsAsLatest method, we will mark the second version as latest.
   * At this point, we should get the second version should be returned when we try to get the latest version.
   *
   * @throws Exception if the test crashes (not expected)
   */
  @Test
  public void testMarkLatestAppsWithTwoVersionsOfSameApp() throws Exception {
    String appName = "testApplicationWithTwoVersions";
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appName);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appWithConfig", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, ConfigTestApp.class);
    // deploy the first version using the public deploy API (without the skipMarkingLatest flag)
    HttpResponse resp1 = deploy(
        appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));
    Assert.assertEquals(200, resp1.getResponseCode());
    String firstVersion = applicationLifecycleService.getAppVersions(
        appId.toEntityId().getAppReference()
    ).stream().collect(Collectors.toList()).get(0);

    // deploy the second version using the internal deploy API (with skipMarkingLatest set to true)
    HttpResponse resp2 = deployWithoutMarkingLatest(
        appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));
    Assert.assertEquals(200, resp2.getResponseCode());
    String secondVersion = applicationLifecycleService.getAppVersions(
        appId.toEntityId().getAppReference()
    ).stream().filter(version -> !version.equals(firstVersion)).collect(Collectors.toList()).get(0);

    // verify that the first version was deployed before the second version
    ApplicationDetail v1Detail = applicationLifecycleService.getAppDetail(
        new ApplicationId(Id.Namespace.DEFAULT.getId(), appName, firstVersion));
    ApplicationDetail v2Detail = applicationLifecycleService.getAppDetail(
        new ApplicationId(Id.Namespace.DEFAULT.getId(), appName, secondVersion));
    Assert.assertTrue(
        v1Detail.getChange().getCreationTimeMillis() < v2Detail.getChange().getCreationTimeMillis());

    // get the latest version of the app. It should be the first version at this point.
    ApplicationDetail latest = applicationLifecycleService.getLatestAppDetail(
        appId.toEntityId().getAppReference());
    Assert.assertEquals(appName, latest.getName());
    Assert.assertEquals(firstVersion, latest.getAppVersion());

    // now mark the second version as latest
    applicationLifecycleService.markAppsAsLatest(
        Namespace.DEFAULT.toEntityId(),
        new MarkLatestAppsRequest(
            Collections.singletonList(new AppVersion(appName, secondVersion))
        ));

    // get the latest version of the app. It should be the second version at this point.
    latest = applicationLifecycleService.getLatestAppDetail(appId.toEntityId().getAppReference());
    Assert.assertEquals(appName, latest.getName());
    Assert.assertEquals(secondVersion, latest.getAppVersion());
  }

  @Test
  public void testUpdateSourceControlMeta() throws Exception {
    // deploy an app, then update its scm meta
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationDetail applicationDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    Assert.assertNull(applicationDetail.getSourceControlMeta());

    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Collections.singletonList(new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                applicationDetail.getAppVersion(),
                "updated-file-hash"
            )), "updated-commit-id")
    );

    ApplicationDetail updatedDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    Assert.assertNotNull(updatedDetail.getSourceControlMeta());
    Assert.assertEquals("updated-file-hash", updatedDetail.getSourceControlMeta().getFileHash());
    Assert.assertEquals("updated-commit-id", updatedDetail.getSourceControlMeta().getCommitId());
  }

  @Test
  public void testUpdateSourceControlMetaWithNonExistingApp() throws Exception {
    // deploy an app, then update its scm meta
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    ApplicationDetail applicationDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    Assert.assertNull(applicationDetail.getSourceControlMeta());

    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Arrays.asList(new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                applicationDetail.getAppVersion(),
                "updated-file-hash"
            ), new UpdateSourceControlMetaRequest(
                "someValidAppNameThatDoesNotExist",
                "some-version",
                "some-file-hash"
            )), "updated-commit-id")
    );

    ApplicationDetail updatedDetail = getAppDetails(TEST_NAMESPACE1, AllProgramsApp.NAME);
    Assert.assertNotNull(updatedDetail.getSourceControlMeta());
    Assert.assertEquals("updated-file-hash", updatedDetail.getSourceControlMeta().getFileHash());
    Assert.assertEquals("updated-commit-id", updatedDetail.getSourceControlMeta().getCommitId());
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateSourceControlMetaWithDuplicateAppIds() throws Exception {
    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Arrays.asList(new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                "-SNAPSHOT",
                "some-file-hash"
            ), new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                "-SNAPSHOT",
                "some-other-file-hash"
            )),
            "commit-id")
    );
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateSourceControlMetaWithInvalidAppId() throws Exception {
    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Collections.singletonList(new UpdateSourceControlMetaRequest(
                "$invalid AppName",
                "-SNAPSHOT",
                "some-file-hash"
            )),
            "commit-id")
    );
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateSourceControlMetaWithNullAppId() throws Exception {
    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Collections.singletonList(new UpdateSourceControlMetaRequest(
                null,
                "-SNAPSHOT",
                "some-file-hash"
            )),
            "commitId")
    );
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateSourceControlMetaWithNullAppVersion() throws Exception {
    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Collections.singletonList(new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                null,
                "some-file-hash"
            )),
            "commitId")
    );
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateSourceControlMetaWithNullGitFileHash() throws Exception {
    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Collections.singletonList(new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                "-SNAPSHOT",
                null
            )),
            "commitId")
    );
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateSourceControlMetaWithEmptyGitFileHash() throws Exception {
    applicationLifecycleService.updateSourceControlMeta(
        new NamespaceId(TEST_NAMESPACE1),
        new UpdateMultiSourceControlMetaReqeust(
            Collections.singletonList(new UpdateSourceControlMetaRequest(
                AllProgramsApp.NAME,
                "-SNAPSHOT",
                ""
            )),
            "commitId")
    );
  }

  private void waitForRuns(int expected, final ProgramId programId, final ProgramRunStatus status) throws Exception {
    Tasks.waitFor(expected, () -> getProgramRuns(Id.Program.fromEntityId(programId), status).size(),
                  5, TimeUnit.SECONDS);
  }

  // creates an application jar for the given application class, but excludes classes that begin with 'com.google.'
  // similar to AppJarHelper#createDeploymentJar
  public static Location createDeploymentJar(LocationFactory locationFactory, Class<?> clz,
                                             File... bundleEmbeddedJars) throws IOException {
    return AppJarHelper.createDeploymentJar(locationFactory, clz, new Manifest(), new ClassAcceptor() {
      final Set<String> visibleResources = ProgramResources.getVisibleResources();

      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (visibleResources.contains(className.replace('.', '/') + ".class")) {
          return false;
        }
        // TODO: Fix it with CDAP-5800
        if (className.startsWith("org.apache.spark.")) {
          return false;
        }
        // exclude a necessary dependency
        if (className.startsWith("com.google.")) {
          return false;
        }
        return true;
      }
    }, bundleEmbeddedJars);
  }

  private void deleteAppAndData(ApplicationId applicationId) throws Exception {
    deleteApp(applicationId, 200);
    deleteNamespaceData(applicationId.getNamespace());
  }
}
