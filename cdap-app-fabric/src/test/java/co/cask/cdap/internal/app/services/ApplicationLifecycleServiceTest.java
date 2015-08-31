/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.AppWithMR;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.artifact.Plugin;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Manifest;

/**
 */
public class ApplicationLifecycleServiceTest extends AppFabricTestBase {

  private static ApplicationLifecycleService applicationLifecycleService;
  private static Store store;
  private static LocationFactory locationFactory;
  private static ArtifactRepository artifactRepository;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    store = getInjector().getInstance(DefaultStore.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
  }

  @Test
  public void testUpgrade() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, "3.1.0");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AppWithMR.class, manifest);

    Application app = new AppWithMR();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
    app.configure(configurer, new DefaultApplicationContext());
    ApplicationSpecification appSpec = configurer.createSpecification();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appSpec.getName());

    try {
      // write an app spec without an artifact id
      store.addApplication(appId, appSpec, appJar);
      appSpec = store.getApplication(appId);
      Assert.assertNotNull(appSpec);
      Assert.assertNull(appSpec.getArtifactId());

      // run upgrade
      applicationLifecycleService.upgrade(false);

      // app spec should now have an artifact id
      appSpec = store.getApplication(appId);
      Assert.assertEquals(new ArtifactId(appSpec.getName(), new ArtifactVersion("3.1.0"), false),
                          appSpec.getArtifactId());

      // run upgrade again to make sure it doesn't break anything
      applicationLifecycleService.upgrade(false);
      appSpec = store.getApplication(appId);
      Assert.assertEquals(new ArtifactId(appSpec.getName(), new ArtifactVersion("3.1.0"), false),
                          appSpec.getArtifactId());
    } finally {
      appJar.delete();
      store.removeApplication(appId);
      artifactRepository.clear(Id.Namespace.DEFAULT);
    }
  }

  @Test
  public void testBadAppLocation() throws Exception {
    String version = "2.8.0-SNAPSHOT";
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, version);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AppWithMR.class, manifest);

    Application app = new AppWithMR();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
    app.configure(configurer, new DefaultApplicationContext());
    ApplicationSpecification appSpec = configurer.createSpecification();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appSpec.getName());

    try {
      // write an app spec without an artifact id
      store.addApplication(appId, appSpec, appJar);
      appSpec = store.getApplication(appId);
      Assert.assertNotNull(appSpec);
      Assert.assertNull(appSpec.getArtifactId());

      // now delete the jar
      appJar.delete();

      // run upgrade, shouldn't choke
      applicationLifecycleService.upgrade(false);

      // app won't be upgraded though. but nothing we can do
      appSpec = store.getApplication(appId);
      Assert.assertNull(appSpec.getArtifactId());
    } finally {
      appJar.delete();
      store.removeApplication(appId);
      artifactRepository.clear(Id.Namespace.DEFAULT);
    }
  }

  @Test
  public void testIdempotency() throws Exception {
    String version = "3.1.0";
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, version);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AppWithMR.class, manifest);

    Application app = new AppWithMR();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
    app.configure(configurer, new DefaultApplicationContext());
    ApplicationSpecification appSpec = configurer.createSpecification();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appSpec.getName());
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, appSpec.getName(), version);

    // simulate a state where update failed half way through
    // the artifact was added, but the app spec was not updated
    try {
      // write an app spec without an artifact id
      store.addApplication(appId, appSpec, appJar);
      appSpec = store.getApplication(appId);
      Assert.assertNotNull(appSpec);
      Assert.assertNull(appSpec.getArtifactId());

      // write artifact to repo
      File jarFile = new File(tmpFolder.newFolder(), "appWithMR-3.1.0.jar");
      Files.copy(Locations.newInputSupplier(appJar), jarFile);
      artifactRepository.addArtifact(artifactId, jarFile);
      jarFile.delete();

      // run upgrade
      applicationLifecycleService.upgrade(false);
      appSpec = store.getApplication(appId);
      Assert.assertEquals(new ArtifactId(artifactId.getName(), artifactId.getVersion(), false),
                          appSpec.getArtifactId());
    } finally {
      appJar.delete();
      store.removeApplication(appId);
      artifactRepository.clear(Id.Namespace.DEFAULT);
    }
  }

  @Test
  public void testTemplatesDeleted() throws Exception {
    Location dummyLocation = locationFactory.create(UUID.randomUUID().toString());
    dummyLocation.mkdirs();
    // manually add app spec for etl batch and realtime
    Map<String, WorkflowSpecification> workflowSpecs = ImmutableMap.of(
      "ETLWorkflow", new WorkflowSpecification(
        "co.cask.cdap.template.etl.batch.ETLWorkflow", "ETLWorkflow", "", Collections.<String, String>emptyMap(),
        ImmutableList.<WorkflowNode>of(
          new WorkflowActionNode("ETLMapReduce",
                                 new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "ETLMapReduce")))
      ));
    Map<String, MapReduceSpecification> mrSpecs = ImmutableMap.of(
      "ETLMapReduce", new MapReduceSpecification(
        "co.cask.cdap.template.etl.batch.ETLMapReduce", "ETLMapReduce", "", null, null,
        Collections.<String>emptySet(), Collections.<String, String>emptyMap(), null, null)
    );
    Map<String, WorkerSpecification> workerSpecs = ImmutableMap.of(
      "ETLWorker", new WorkerSpecification(
        "co.cask.cdap.template.etl.realtime.ETLWorker", "ETLWorker", "", Collections.<String, String>emptyMap(),
        Collections.<String>emptySet(), null, 1)
    );
    ApplicationSpecification etlBatchSpec = new DefaultApplicationSpecification(
      "ETLBatch", "", "",
      null,
      Collections.<String, StreamSpecification>emptyMap(),
      Collections.<String, String>emptyMap(),
      Collections.<String, DatasetCreationSpec>emptyMap(),
      Collections.<String, FlowSpecification>emptyMap(),
      mrSpecs,
      Collections.<String, SparkSpecification>emptyMap(),
      workflowSpecs,
      Collections.<String, ServiceSpecification>emptyMap(),
      Collections.<String, ScheduleSpecification>emptyMap(),
      Collections.<String, WorkerSpecification>emptyMap(),
      Collections.<String, Plugin>emptyMap()
    );
    ApplicationSpecification etlRealtimeSpec = new DefaultApplicationSpecification(
      "ETLRealtime", "", "",
      null,
      Collections.<String, StreamSpecification>emptyMap(),
      Collections.<String, String>emptyMap(),
      Collections.<String, DatasetCreationSpec>emptyMap(),
      Collections.<String, FlowSpecification>emptyMap(),
      Collections.<String, MapReduceSpecification>emptyMap(),
      Collections.<String, SparkSpecification>emptyMap(),
      Collections.<String, WorkflowSpecification>emptyMap(),
      Collections.<String, ServiceSpecification>emptyMap(),
      Collections.<String, ScheduleSpecification>emptyMap(),
      workerSpecs,
      Collections.<String, Plugin>emptyMap()
    );
    Id.Application etlBatchId = Id.Application.from(Id.Namespace.DEFAULT, "ETLBatch");
    Id.Application etlRealtimeId = Id.Application.from(Id.Namespace.DEFAULT, "ETLRealtime");

    store.addApplication(etlBatchId, etlBatchSpec, dummyLocation);
    store.addApplication(etlRealtimeId, etlRealtimeSpec, dummyLocation);

    // run upgrade
    applicationLifecycleService.upgrade(false);

    // make sure apps are gone
    Assert.assertNull(store.getApplication(etlBatchId));
    Assert.assertNull(store.getApplication(etlRealtimeId));
  }
}
