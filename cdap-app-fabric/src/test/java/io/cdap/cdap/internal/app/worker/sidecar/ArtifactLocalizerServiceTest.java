/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.service.RetryStrategyType;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Unit test for {@link ArtifactLocalizerService}.
 */
public class ArtifactLocalizerServiceTest extends AppFabricTestBase {

  private ArtifactLocalizerService localizerService;
  private CConfiguration cConf;

  private CConfiguration createCConf(int port) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, port);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);

    String prefix = "task.worker.";
    cConf.set(prefix + Constants.Retry.TYPE, RetryStrategyType.FIXED_DELAY.toString());
    cConf.set(prefix + Constants.Retry.MAX_RETRIES, "100");
    cConf.set(prefix + Constants.Retry.MAX_TIME_SECS, "10");
    cConf.set(prefix + Constants.Retry.DELAY_BASE_MS, "200");
    return cConf;
  }

  private ArtifactLocalizerService setupArtifactLocalizerService(int port) throws IOException {
    cConf = createCConf(port);

    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);

    String tempFolderPath = tmpFolder.newFolder().getPath();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolderPath);
    RemoteClientFactory remoteClientFactory =
      new RemoteClientFactory(discoveryClient, new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    ArtifactLocalizerService artifactLocalizerService =
      new ArtifactLocalizerService(cConf, new ArtifactLocalizer(cConf, remoteClientFactory));
    // start the service
    artifactLocalizerService.startAndWait();

    return artifactLocalizerService;
  }

  @Before
  public void setUp() throws Exception {
    this.localizerService = setupArtifactLocalizerService(10001);
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
    ArtifactLocalizerClient client = new ArtifactLocalizerClient(cConf);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    File newAppJarFile = new File(tmpFolder.newFolder(),
                                  String.format("%s-%s-copy.jar", artifactId.getName(),
                                                artifactId.getVersion().getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);

    File unpackedDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());

    // Make sure the artifact was actually cached
    validateUnpackDir(unpackedDir);

    // Call the sidecar again and make sure the same path was returned
    File sameUnpackedDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());
    Assert.assertEquals(unpackedDir, sameUnpackedDir);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);

    Thread.sleep(1000);
    Files.copy(appJarFile, newAppJarFile);
    artifactRepository.addArtifact(artifactId, newAppJarFile);

    File newUnpackDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());

    //Make sure the two paths arent the same and that the old one is gone
    Assert.assertNotEquals(unpackedDir, newUnpackDir);
    validateUnpackDir(newUnpackDir);
  }

  private void validateUnpackDir(File unpackedFile) {
    // Make sure the directory exists
    Assert.assertTrue(unpackedFile.exists());
    Assert.assertTrue(unpackedFile.isDirectory());

    // Make sure theres multiple files in the directory and one of them is a manifest
    List<String> files = DirUtils.list(unpackedFile);
    Assert.assertTrue(files.size() > 1);
    Assert.assertTrue(files.stream().anyMatch(s -> s.equals("META-INF")));
  }
}
