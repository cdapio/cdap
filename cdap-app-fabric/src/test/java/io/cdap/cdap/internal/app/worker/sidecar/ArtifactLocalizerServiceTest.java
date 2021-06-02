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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.io.Files;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Unit test for {@link ArtifactLocalizerService}.
 */
public class ArtifactLocalizerServiceTest extends AppFabricTestBase {

  private ArtifactLocalizerService localizerService;

  private CConfiguration createCConf(int port) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, port);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    return cConf;
  }

  private ArtifactLocalizerService setupFileLocalizerService(int port) throws IOException {
    CConfiguration cConf = createCConf(port);
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);
    ArtifactLocalizerService artifactLocalizerService =
      new ArtifactLocalizerService(cConf, new InMemoryDiscoveryService(),
                                   new ArtifactLocalizer(discoveryClient,
                                                         tmpFolder.newFolder()));
    // start the service
    artifactLocalizerService.startAndWait();

    return artifactLocalizerService;
  }

  @Before
  public void setUp() throws Exception {
    this.localizerService = setupFileLocalizerService(10001);
    getInjector().getInstance(ArtifactRepository.class).clear(NamespaceId.DEFAULT);
  }

  @After
  public void tearDown() throws Exception {
    this.localizerService.shutDown();
  }

  @Test
  public void testUnpackArtifact() throws Exception {

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    ArtifactLocalizerClient client = getInjector().getInstance(ArtifactLocalizerClient.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    File newAppJarFile = new File(tmpFolder.newFolder(),
                                  String.format("%s-%s-copy.jar", artifactId.getName(),
                                                artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);

    Location unpackedDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());

    // Make sure the artifact was actually cached
    validateUnpackDir(unpackedDir);

    // Call the sidecar again and make sure the same path was returned
    Location sameUnpackedDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());
    Assert.assertEquals(unpackedDir, sameUnpackedDir);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);

    Thread.sleep(1000);
    Files.copy(appJarFile, newAppJarFile);
    artifactRepository.addArtifact(artifactId, newAppJarFile);

    Location newUnpackDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());

    //Make sure the two paths arent the same and that the old one is gone
    Assert.assertNotEquals(unpackedDir, newUnpackDir);
    validateUnpackDir(newUnpackDir);
    Assert.assertFalse(unpackedDir.exists());
  }

  private void validateUnpackDir(Location dirPath) {
    File unpackedFile = Paths.get(dirPath.toURI()).toFile();

    // Make sure the directory exists
    Assert.assertTrue(unpackedFile.exists());
    Assert.assertTrue(unpackedFile.isDirectory());

    // Make sure theres multiple files in the directory and one of them is a manifest
    String[] fileNames = unpackedFile.list();
    Assert.assertTrue(fileNames.length > 1);
    Assert.assertTrue(Arrays.stream(fileNames).anyMatch(s -> s.equals("META-INF")));
  }

  @Test
  public void testArtifact() throws Exception {

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    ArtifactLocalizerClient client = getInjector().getInstance(ArtifactLocalizerClient.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    File newAppJarFile = new File(tmpFolder.newFolder(),
                                  String.format("%s-%s-copy.jar", artifactId.getName(),
                                                artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);

    Location artifactPath = client.getArtifactLocation(artifactId.toEntityId());

    // Make sure the artifact was actually cached
    Assert.assertTrue(artifactPath.exists());

    // Call the sidecar again and make sure the same path was returned
    Location sameArtifactPath = client.getArtifactLocation(artifactId.toEntityId());
    Assert.assertEquals(artifactPath, sameArtifactPath);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);

    // Wait a bit before recreating the artifact to make sure the last modified time is different
        Thread.sleep(1000);
    Files.copy(appJarFile, newAppJarFile);
    artifactRepository.addArtifact(artifactId, newAppJarFile);

    Location newArtifactPath = client.getArtifactLocation(artifactId.toEntityId());

    //Make sure the two paths arent the same and that the old one is gone
    Assert.assertNotEquals(artifactPath, newArtifactPath);
    Assert.assertTrue(newArtifactPath.exists());
    Assert.assertFalse(artifactPath.exists());
  }
}
