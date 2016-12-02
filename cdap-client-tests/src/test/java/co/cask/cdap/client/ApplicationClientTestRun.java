/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.Config;
import co.cask.cdap.client.app.AppReturnsArgs;
import co.cask.cdap.client.app.ConfigTestApp;
import co.cask.cdap.client.app.ConfigurableProgramsApp;
import co.cask.cdap.client.app.ConfigurableProgramsApp2;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeDatasetModule;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.DatasetModuleNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    Id.Namespace namespace = Id.Namespace.DEFAULT;

    try {
      datasetClient.delete(Id.DatasetInstance.from(namespace, FakeApp.DS_NAME));
    } catch (DatasetNotFoundException e) {
      // NO-OP
    }

    try {
      datasetModuleClient.delete(Id.DatasetModule.from(namespace, FakeDatasetModule.NAME));
    } catch (DatasetModuleNotFoundException e) {
      // NO-OP
    }
  }

  @Test(expected = IOException.class)
  public void testInvalidAppConfig() throws Exception {
    Id.Application appid = Id.Application.from(Id.Namespace.DEFAULT, ConfigTestApp.NAME);
    appClient.deploy(appid.getNamespace(),
                     createAppJarFile(ConfigTestApp.class, ConfigTestApp.NAME, "1.0.0-SNAPSHOT"), "adad");
  }

  @Test
  public void testAll() throws Exception {
    Id.Application app = Id.Application.from(Id.Namespace.DEFAULT, FakeApp.NAME);

    Assert.assertEquals(0, appClient.list(Id.Namespace.DEFAULT).size());

    // deploy app
    LOG.info("Deploying app");
    appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(FakeApp.class, FakeApp.NAME, "1.0.0-SNAPSHOT"));
    appClient.waitForDeployed(app, 30, TimeUnit.SECONDS);
    Assert.assertEquals(1, appClient.list(Id.Namespace.DEFAULT).size());

    try {
      // check program list
      LOG.info("Checking program list for app");
      Map<ProgramType, List<ProgramRecord>> programs = appClient.listProgramsByType(app);
      verifyProgramNames(FakeApp.FLOWS, programs.get(ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, programs.get(ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, programs.get(ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, programs.get(ProgramType.SERVICE));

      verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(app, ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listPrograms(app, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listPrograms(app, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listPrograms(app, ProgramType.SERVICE));

      verifyProgramNames(FakeApp.FLOWS, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.SERVICE));

      verifyProgramRecords(FakeApp.ALL_PROGRAMS, appClient.listAllPrograms(Id.Namespace.DEFAULT));

      ApplicationDetail appDetail = appClient.get(app);
      ArtifactSummary expected = new ArtifactSummary(FakeApp.NAME, "1.0.0-SNAPSHOT");
      Assert.assertEquals(expected, appDetail.getArtifact());
    } finally {
      // delete app
      LOG.info("Deleting app");
      appClient.delete(app);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(Id.Namespace.DEFAULT).size());
    }
  }

  @Test
  public void testAppConfig() throws Exception {
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("testStream", "testDataset");
    appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(ConfigTestApp.class), config);
    Assert.assertEquals(1, appClient.list(Id.Namespace.DEFAULT).size());

    Id.Application app = Id.Application.from(Id.Namespace.DEFAULT, ConfigTestApp.NAME);
    try {
      appClient.exists(app);
    } finally {
      appClient.delete(app);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(Id.Namespace.DEFAULT).size());
    }
  }

  @Test
  public void testAppUpdate() throws Exception {
    String artifactName = "cfg-programs";
    ArtifactId artifactIdV1 = NamespaceId.DEFAULT.artifact(artifactName, "1.0.0");
    ArtifactId artifactIdV2 = NamespaceId.DEFAULT.artifact(artifactName, "2.0.0");
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "ProgramsApp");

    artifactClient.add(NamespaceId.DEFAULT, artifactName,
                       Files.newInputStreamSupplier(createAppJarFile(ConfigurableProgramsApp.class)),
                       "1.0.0");
    artifactClient.add(NamespaceId.DEFAULT, artifactName,
      Files.newInputStreamSupplier(createAppJarFile(ConfigurableProgramsApp2.class)),
      "2.0.0");

    try {
      // deploy the app with just the worker
      ConfigurableProgramsApp.Programs conf =
        new ConfigurableProgramsApp.Programs(null, "worker1", "stream1", "dataset1");
      AppRequest<ConfigurableProgramsApp.Programs> request = new AppRequest<>(
        new ArtifactSummary(artifactIdV1.getEntityName(), artifactIdV1.getVersion()), conf);
      appClient.deploy(appId, request);

      // should only have the worker
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.FLOW).isEmpty());
      Assert.assertEquals(1, appClient.listPrograms(appId, ProgramType.WORKER).size());

      // update to use just the flow
      conf = new ConfigurableProgramsApp.Programs("flow1", null, "stream1", "dataset1");
      request = new AppRequest<>(
        new ArtifactSummary(artifactIdV1.getEntityName(), artifactIdV1.getVersion()), conf);
      appClient.update(appId, request);

      // should only have the flow
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.WORKER).isEmpty());
      Assert.assertEquals(1, appClient.listPrograms(appId, ProgramType.FLOW).size());

      // check nonexistent app is not found
      try {
        appClient.update(Id.Application.from(Id.Namespace.DEFAULT, "ghost"), request);
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
        new ArtifactSummary(artifactIdV1.getEntityName(), "0.0.1"), conf);
      try {
        appClient.update(appId, request);
        Assert.fail();
      } catch (NotFoundException e) {
        // expected
      }

      // update artifact version. This version uses a different app class with that can add a service
      ConfigurableProgramsApp2.Programs conf2 =
        new ConfigurableProgramsApp2.Programs(null, null, "stream1", "dataset1", "service2");
      AppRequest<ConfigurableProgramsApp2.Programs> request2 = new AppRequest<>(
        new ArtifactSummary(artifactIdV2.getEntityName(), artifactIdV2.getVersion()), conf2);
      appClient.update(appId, request2);

      // should only have a single service
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.WORKER).isEmpty());
      Assert.assertTrue(appClient.listPrograms(appId, ProgramType.FLOW).isEmpty());
      Assert.assertEquals(1, appClient.listPrograms(appId, ProgramType.SERVICE).size());
    } finally {
      appClient.delete(appId);
      appClient.waitForDeleted(appId, 30, TimeUnit.SECONDS);
      artifactClient.delete(artifactIdV1);
      artifactClient.delete(artifactIdV2);
    }
  }

  @Test
  public void testArtifactFilter() throws Exception {
    Id.Application appId1 = Id.Application.from(Id.Namespace.DEFAULT, FakeApp.NAME);
    Id.Application appId2 = Id.Application.from(Id.Namespace.DEFAULT, "fake2");
    Id.Application appId3 = Id.Application.from(Id.Namespace.DEFAULT, "fake3");
    try {
      // app1 should use fake-1.0.0-SNAPSHOT
      appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(FakeApp.class, "otherfake", "1.0.0-SNAPSHOT"));
      appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(FakeApp.class, "fake", "0.1.0-SNAPSHOT"));
      // app1 should end up with fake-1.0.0-SNAPSHOT
      appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(FakeApp.class, "fake", "1.0.0-SNAPSHOT"));
      // app2 should use fake-0.1.0-SNAPSHOT
      appClient.deploy(appId2, new AppRequest<Config>(new ArtifactSummary("fake", "0.1.0-SNAPSHOT")));
      // app3 should use otherfake-1.0.0-SNAPSHOT
      appClient.deploy(appId3, new AppRequest<Config>(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT")));
      appClient.waitForDeployed(appId1, 30, TimeUnit.SECONDS);
      appClient.waitForDeployed(appId2, 30, TimeUnit.SECONDS);
      appClient.waitForDeployed(appId3, 30, TimeUnit.SECONDS);

      // check calls that should return nothing
      // these don't match anything
      Assert.assertTrue(appClient.list(Id.Namespace.DEFAULT, "ghost", null).isEmpty());
      Assert.assertTrue(appClient.list(Id.Namespace.DEFAULT, (String) null, "1.0.0").isEmpty());
      Assert.assertTrue(appClient.list(Id.Namespace.DEFAULT, "ghost", "1.0.0").isEmpty());
      // these match one but not the other
      Assert.assertTrue(appClient.list(Id.Namespace.DEFAULT, "otherfake", "0.1.0-SNAPSHOT").isEmpty());
      Assert.assertTrue(appClient.list(Id.Namespace.DEFAULT, "fake", "1.0.0").isEmpty());

      // check filter by name only
      Set<ApplicationRecord> apps = Sets.newHashSet(appClient.list(Id.Namespace.DEFAULT, "fake", null));
      Set<ApplicationRecord> expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "1.0.0-SNAPSHOT"), appId1.toEntityId(), ""),
        new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"), appId2.toEntityId(), "")
      );
      Assert.assertEquals(expected, apps);

      apps = Sets.newHashSet(appClient.list(Id.Namespace.DEFAULT, "otherfake", null));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT"), appId3.toEntityId(), ""));
      Assert.assertEquals(expected, apps);

      // check filter by multiple names
      apps = Sets.newHashSet(appClient.list(Id.Namespace.DEFAULT, ImmutableSet.of("fake", "otherfake"), null));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT"), appId3.toEntityId(), ""),
        new ApplicationRecord(new ArtifactSummary("fake", "1.0.0-SNAPSHOT"), appId1.toEntityId(), ""),
        new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"), appId2.toEntityId(), ""));
      Assert.assertEquals(expected, apps);

      // check filter by version only
      apps = Sets.newHashSet(appClient.list(Id.Namespace.DEFAULT, (String) null, "0.1.0-SNAPSHOT"));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"), appId2.toEntityId(), "")
      );
      Assert.assertEquals(expected, apps);

      apps = Sets.newHashSet(appClient.list(Id.Namespace.DEFAULT, (String) null, "1.0.0-SNAPSHOT"));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "1.0.0-SNAPSHOT"), appId1.toEntityId(), ""),
        new ApplicationRecord(new ArtifactSummary("otherfake", "1.0.0-SNAPSHOT"), appId3.toEntityId(), "")
      );
      Assert.assertEquals(expected, apps);

      // check filter by both
      apps = Sets.newHashSet(appClient.list(Id.Namespace.DEFAULT, "fake", "0.1.0-SNAPSHOT"));
      expected = ImmutableSet.of(
        new ApplicationRecord(new ArtifactSummary("fake", "0.1.0-SNAPSHOT"), appId2.toEntityId(), "")
      );
      Assert.assertEquals(expected, apps);
    } finally {
      appClient.deleteAll(Id.Namespace.DEFAULT);
      appClient.waitForDeleted(appId1, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(appId2, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(appId3, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(Id.Namespace.DEFAULT).size());
    }
  }

  @Test
  public void testDeleteAll() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;
    Id.Application app = Id.Application.from(namespace, FakeApp.NAME);
    Id.Application app2 = Id.Application.from(namespace, AppReturnsArgs.NAME);

    Assert.assertEquals(0, appClient.list(namespace).size());

    try {
      // deploy first app
      LOG.info("Deploying first app");
      appClient.deploy(namespace, createAppJarFile(FakeApp.class));
      appClient.waitForDeployed(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(1, appClient.list(namespace).size());

      // deploy second app
      LOG.info("Deploying second app");
      appClient.deploy(namespace, createAppJarFile(AppReturnsArgs.class));
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
