/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.client.app.AppReturnsArgs;
import io.cdap.cdap.client.app.ConfigurableProgramsApp;
import io.cdap.cdap.client.app.ConfigurableProgramsApp2;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.app.FakeDatasetModule;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.DatasetModuleNotFoundException;
import io.cdap.cdap.common.DatasetNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.XSlowTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link ApplicationClient}.
 */
@Category(XSlowTests.class)
public class ApplicationClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationClientTestRun.class);

  private ApplicationClient appClient;
  private DatasetClient datasetClient;
  private DatasetModuleClient datasetModuleClient;
  private ArtifactClient artifactClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    datasetClient = new DatasetClient(clientConfig);
    datasetModuleClient = new DatasetModuleClient(clientConfig);
    artifactClient = new ArtifactClient(clientConfig);
  }

  @After
  public void cleanup() throws Throwable {
    // Delete FakeApp's dataset and module so that DatasetClientTestRun works when running both inside a test suite
    // This is due to DatasetClientTestRun assuming that it is using a blank CDAP instance

    try {
      datasetClient.delete(NamespaceId.DEFAULT.dataset(FakeApp.DS_NAME));
    } catch (DatasetNotFoundException e) {
      // NO-OP
    }

    try {
      datasetModuleClient.delete(NamespaceId.DEFAULT.datasetModule(FakeDatasetModule.NAME));
    } catch (DatasetModuleNotFoundException e) {
      // NO-OP
    }
  }

  @Test(expected = IOException.class)
  public void testInvalidAppConfig() throws Exception {
    ApplicationId appid = NamespaceId.DEFAULT.app(ConfigTestApp.NAME);
    appClient.deploy(appid.getParent(),
                     createAppJarFile(ConfigTestApp.class, ConfigTestApp.NAME, "1.0.0-SNAPSHOT"), "adad");
  }

  @Test
  public void testAll() throws Exception {
    ApplicationId app = NamespaceId.DEFAULT.app(FakeApp.NAME);

    Assert.assertEquals(0, appClient.list(NamespaceId.DEFAULT).size());

    // deploy app
    LOG.info("Deploying app");
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(FakeApp.class, FakeApp.NAME, "1.0.0-SNAPSHOT"));
    ApplicationDetail appDetail = appClient.get(app);
    app = new ApplicationId(app.getNamespace(), app.getApplication(), appDetail.getAppVersion());
    ArtifactSummary expected = new ArtifactSummary(FakeApp.NAME, "1.0.0-SNAPSHOT");
    appClient.waitForDeployed(app, 30, TimeUnit.SECONDS);
    Assert.assertEquals(1, appClient.list(NamespaceId.DEFAULT).size());

    try {
      // check program list
      LOG.info("Checking program list for app");
      Map<ProgramType, List<ProgramRecord>> programs = appClient.listProgramsByType(app);
      verifyProgramNames(FakeApp.MAPREDUCES, programs.get(ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, programs.get(ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, programs.get(ProgramType.SERVICE));

      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listPrograms(app, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listPrograms(app, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listPrograms(app, ProgramType.SERVICE));

      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listAllPrograms(NamespaceId.DEFAULT, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listAllPrograms(NamespaceId.DEFAULT, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listAllPrograms(NamespaceId.DEFAULT, ProgramType.SERVICE));

      verifyProgramRecords(FakeApp.ALL_PROGRAMS, appClient.listAllPrograms(NamespaceId.DEFAULT));


      Assert.assertEquals(expected, appDetail.getArtifact());
    } finally {
      // delete app
      LOG.info("Deleting app");
      appClient.deleteApp(app);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(NamespaceId.DEFAULT).size());
    }
  }

  @Test
  public void testAppConfig() throws Exception {
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("testDataset");
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(ConfigTestApp.class), config);
    ApplicationId app = NamespaceId.DEFAULT.app(ConfigTestApp.NAME);
    ApplicationDetail appDetail = appClient.get(app);
    Assert.assertEquals(1, appClient.list(NamespaceId.DEFAULT).size());
    app = new ApplicationId(app.getNamespace(), app.getApplication(), appDetail.getAppVersion());

    try {
      appClient.exists(app);
    } finally {
      appClient.deleteApp(app);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(NamespaceId.DEFAULT).size());
    }
  }

  @Test
  public void testAppUpdate() throws Exception {
    String artifactName = "cfg-programs";
    ArtifactId artifactIdV1 = NamespaceId.DEFAULT.artifact(artifactName, "1.0.0");
    ArtifactId artifactIdV2 = NamespaceId.DEFAULT.artifact(artifactName, "2.0.0");
    ApplicationId appId = NamespaceId.DEFAULT.app("ProgramsApp");

    artifactClient.add(NamespaceId.DEFAULT, artifactName,
                       () -> Files.newInputStream(createAppJarFile(ConfigurableProgramsApp.class).toPath()),
                       "1.0.0");
    artifactClient.add(NamespaceId.DEFAULT, artifactName,
                       () -> Files.newInputStream(createAppJarFile(ConfigurableProgramsApp2.class).toPath()),
      "2.0.0");

    try {
      // deploy the app with just the worker
      ConfigurableProgramsApp.Programs conf =
        new ConfigurableProgramsApp.Programs("worker1", null, "dataset1");
      AppRequest<ConfigurableProgramsApp.Programs> request = new AppRequest<>(
        new ArtifactSummary(artifactIdV1.getArtifact(), artifactIdV1.getVersion()), conf);
      appClient.deploy(appId, request);
      ApplicationDetail info = appClient.get(appId);
      appId = new ApplicationId(appId.getNamespace(), appId.getApplication(), info.getAppVersion());

      // should only have the worker
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.SERVICE).isEmpty());
      Assert.assertEquals(1, appClient.listPrograms(appId, ProgramType.WORKER).size());

      // update to use just the service
      conf = new ConfigurableProgramsApp.Programs(null, "service", "dataset1");
      request = new AppRequest<>(
        new ArtifactSummary(artifactIdV1.getArtifact(), artifactIdV1.getVersion()), conf);
      appClient.update(appId, request);
      info = appClient.get(appId);
      appId = new ApplicationId(appId.getNamespace(), appId.getApplication(), info.getAppVersion());

      // should only have the service
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.WORKER).isEmpty());
      Assert.assertEquals(1, appClient.listPrograms(appId, ProgramType.SERVICE).size());

      // check nonexistent app is not found
      try {
        appClient.update(NamespaceId.DEFAULT.app("ghost"), request);
        Assert.fail();
      } catch (NotFoundException e) {
        // expected
      }

      // check different artifact name is invalid
      request = new AppRequest<>(
        new ArtifactSummary("ghost", artifactIdV1.getVersion()), conf);
      try {
        appClient.update(appId, request);
        Assert.fail();
      } catch (BadRequestException e) {
        // expected
      }

      // check nonexistent artifact is not found
      request = new AppRequest<>(
        new ArtifactSummary(artifactIdV1.getArtifact(), "0.0.1"), conf);
      try {
        appClient.update(appId, request);
        Assert.fail();
      } catch (NotFoundException e) {
        // expected
      }

      // update artifact version. This version uses a different app class with that can add a workflow
      ConfigurableProgramsApp2.Programs conf2 =
        new ConfigurableProgramsApp2.Programs(null, null, "workflow1", "dataset1");
      AppRequest<ConfigurableProgramsApp2.Programs> request2 = new AppRequest<>(
        new ArtifactSummary(artifactIdV2.getArtifact(), artifactIdV2.getVersion()), conf2);
      appClient.update(appId, request2);
      info = appClient.get(appId);
      appId = new ApplicationId(appId.getNamespace(), appId.getApplication(), info.getAppVersion());

      // should only have a single workflow
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.WORKER).isEmpty());
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.SERVICE).isEmpty());
      Assert.assertEquals(1, appClient.listPrograms(appId, ProgramType.WORKFLOW).size());
    } finally {
      // updating an app will create a new version, so here we need to delete both versions
      appClient.deleteApp(appId);
      appClient.waitForDeleted(appId, 30, TimeUnit.SECONDS);
      artifactClient.delete(artifactIdV1);
      artifactClient.delete(artifactIdV2);
    }
  }

  @Test
  public void testArtifactFilter() throws Exception {
    ApplicationId appId1 = NamespaceId.DEFAULT.app(FakeApp.NAME);
    ApplicationId appId2 = NamespaceId.DEFAULT.app("fake2");
    ApplicationId appId3 = NamespaceId.DEFAULT.app("fake3");
    try {
      // app1 should use fake-1.0.0-SNAPSHOT
      appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(FakeApp.class, "otherfake", "1.0.0-SNAPSHOT"));
      String version = appClient.listAppVersions(NamespaceId.DEFAULT, FakeApp.NAME).get(0);
      ApplicationDetail otherFakeAppDetail = appClient.get(new ApplicationId
                                                             (NamespaceId.DEFAULT.getNamespace(),
                                                              FakeApp.NAME, version));
      appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(FakeApp.class, "fake", "0.1.0-SNAPSHOT"));
      version = appClient.listAppVersions(NamespaceId.DEFAULT, FakeApp.NAME).get(0);
      ApplicationDetail fakeAppDetail = appClient.get(new ApplicationId(NamespaceId.DEFAULT.getNamespace(),
                                                                        FakeApp.NAME, version));
      // app1 should end up with fake-1.0.0-SNAPSHOT
      appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(FakeApp.class, "fake", "1.0.0-SNAPSHOT"));
      version = appClient.listAppVersions(NamespaceId.DEFAULT, FakeApp.NAME).get(0);
      ApplicationDetail fakeAppDetail2 = appClient.get(new ApplicationId(NamespaceId.DEFAULT.getNamespace(),
                                                                         FakeApp.NAME, version));

      // app2 should use fake-0.1.0-SNAPSHOT
      appClient.deploy(appId2, new AppRequest<Config>(new ArtifactSummary("fake", "0.1.0-SNAPSHOT")));
      version = appClient.listAppVersions(NamespaceId.DEFAULT, "fake2").get(0);
      ApplicationDetail appId2Detail = appClient.get(new ApplicationId(appId2.getNamespace(), appId2.getApplication(),
                                                                       version));
      // app3 should use otherfake-1.0.0-SNAPSHOT
      appClient.deploy(appId3, new AppRequest<Config>(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT")));
      version = appClient.listAppVersions(NamespaceId.DEFAULT, "fake3").get(0);
      ApplicationDetail appId3Detail = appClient.get(new ApplicationId(appId3.getNamespace(), appId3.getApplication(),
                                                                       version));
      appClient.waitForDeployed(appId1, 30, TimeUnit.SECONDS);
      appClient.waitForDeployed(appId2, 30, TimeUnit.SECONDS);
      appClient.waitForDeployed(appId3, 30, TimeUnit.SECONDS);

      // check calls that should return nothing
      // these don't match anything
      Assert.assertTrue(appClient.list(NamespaceId.DEFAULT, "ghost", null).isEmpty());
      Assert.assertTrue(appClient.list(NamespaceId.DEFAULT, (String) null, "1.0.0").isEmpty());
      Assert.assertTrue(appClient.list(NamespaceId.DEFAULT, "ghost", "1.0.0").isEmpty());
      // these match one but not the other
      Assert.assertTrue(appClient.list(NamespaceId.DEFAULT, "otherfake", "0.1.0-SNAPSHOT").isEmpty());
      Assert.assertTrue(appClient.list(NamespaceId.DEFAULT, "fake", "1.0.0").isEmpty());

      // check filter by name only
      Set<ApplicationRecord> apps = Sets.newHashSet(appClient.list(NamespaceId.DEFAULT, "fake", null));
      Set<ApplicationRecord> expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "1.0.0-SNAPSHOT"), appId1.getApplication(),
                              fakeAppDetail2.getAppVersion(), "", null, fakeAppDetail2.getChange()),
        new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"), appId2.getApplication(),
                              appId2Detail.getAppVersion(), "", null, appId2Detail.getChange())
      );
      Assert.assertEquals(expected, apps);

      apps = Sets.newHashSet(appClient.list(NamespaceId.DEFAULT, "otherfake", null));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT"), appId3.getApplication(),
                              appId3Detail.getAppVersion(), "", null, appId3Detail.getChange())
      );
      Assert.assertEquals(expected, apps);

      // check filter by multiple names
      apps = Sets.newHashSet(appClient.list(NamespaceId.DEFAULT, ImmutableSet.of("fake", "otherfake"), null));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "1.0.0-SNAPSHOT"), appId1.getApplication(),
                              fakeAppDetail2.getAppVersion(), "", null, fakeAppDetail2.getChange()),
        new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"), appId2.getApplication(),
                              appId2Detail.getAppVersion(), "", null, appId2Detail.getChange()),
        new ApplicationRecord(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT"), appId3.getApplication(),
                              appId3Detail.getAppVersion(), "", null, appId3Detail.getChange())
      );
      Assert.assertEquals(expected, apps);

      // check filter by version only
      apps = Sets.newHashSet(appClient.list(NamespaceId.DEFAULT, (String) null, "0.1.0-SNAPSHOT"));
      expected = ImmutableSet.of(new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"),
                                                       appId2.getApplication(), appId2Detail.getAppVersion(),
                                                       "", null, appId2Detail.getChange())
      );
      Assert.assertEquals(expected, apps);

      apps = Sets.newHashSet(appClient.list(NamespaceId.DEFAULT, (String) null, "1.0.0-SNAPSHOT"));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "1.0.0-SNAPSHOT"), appId1.getApplication(),
                              fakeAppDetail2.getAppVersion(), "", null, fakeAppDetail2.getChange()),
        new ApplicationRecord(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT"), appId3.getApplication(),
                              appId3Detail.getAppVersion(), "", null, appId3Detail.getChange())
      );
      Assert.assertEquals(expected, apps);

      // check filter by both
      apps = Sets.newHashSet(appClient.list(NamespaceId.DEFAULT, "fake", "0.1.0-SNAPSHOT"));
      expected = ImmutableSet.of(new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"),
                                                       appId2.getApplication(), appId2Detail.getAppVersion(),
                                                       "", null, appId2Detail.getChange()));
      Assert.assertEquals(expected, apps);
    } finally {
      //appClient.deleteAll(NamespaceId.DEFAULT);
      appClient.deleteApp(appId1);
      appClient.deleteApp(appId2);
      appClient.deleteApp(appId3);
      appClient.waitForDeleted(appId1, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(appId2, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(appId3, 30, TimeUnit.SECONDS);
      List<ApplicationRecord> list = appClient.list(NamespaceId.DEFAULT);
      Assert.assertEquals(0, appClient.list(NamespaceId.DEFAULT).size());
    }
  }

  @Test
  public void testDeleteAll() throws Exception {
    NamespaceId namespace = NamespaceId.DEFAULT;
    ApplicationId app = namespace.app(FakeApp.NAME);
    ApplicationId app2 = namespace.app(AppReturnsArgs.NAME);

    Assert.assertEquals(0, appClient.list(namespace).size());

    try {
      // deploy first app
      LOG.info("Deploying first app");
      appClient.deploy(namespace, createAppJarFile(FakeApp.class));
      ApplicationDetail appDetail = appClient.get(app);
      app = new ApplicationId(app.getNamespace(), app.getApplication(), appDetail.getAppVersion());
      appClient.waitForDeployed(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(1, appClient.list(namespace).size());

      // deploy second app
      LOG.info("Deploying second app");
      appClient.deploy(namespace, createAppJarFile(AppReturnsArgs.class));
      ApplicationDetail app2Detail = appClient.get(app);
      app = new ApplicationId(app2.getNamespace(), app2.getApplication(), app2Detail.getAppVersion());
      appClient.waitForDeployed(app2, 30, TimeUnit.SECONDS);
      Assert.assertEquals(2, appClient.list(namespace).size());
    } finally {
      appClient.deleteAll(namespace);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(app2, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(namespace).size());
    }
  }
}
