/*
 * Copyright © 2021-2022 Cask Data, Inc.
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
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link ArtifactLocalizerService}.
 */
public class ArtifactLocalizerServiceTest extends AppFabricTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private ArtifactLocalizerService localizerService;
  private CConfiguration cConf;

  private CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.setInt(Constants.ArtifactLocalizer.CACHE_CLEANUP_INTERVAL_MIN, 60);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    return cConf;
  }

  private ArtifactLocalizerService setupArtifactLocalizerService(CConfiguration cConf) {
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);

    RemoteClientFactory remoteClientFactory =
      new RemoteClientFactory(discoveryClient, new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    ArtifactLocalizerService artifactLocalizerService = new ArtifactLocalizerService(
      cConf, new ArtifactLocalizer(cConf, remoteClientFactory, (namespaceId, retryStrategy) -> {
      return new NoOpArtifactManager();
    }), new CommonNettyHttpServiceFactory(cConf, new NoOpMetricsCollectionService(), auditLogContexts -> {}),
        remoteClientFactory, new NoOpRemoteAuthenticator());
    // start the service
    artifactLocalizerService.startAndWait();

    return artifactLocalizerService;
  }

  @Before
  public void setUp() throws Exception {
    cConf = createCConf();
    localizerService = setupArtifactLocalizerService(cConf);
    getInjector().getInstance(ArtifactRepository.class).clear(NamespaceId.DEFAULT);
  }

  @After
  public void tearDown() throws Exception {
    this.localizerService.stopAndWait();
  }

  @Test
  public void testUnpackArtifact() throws Exception {

    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());

    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    ArtifactLocalizerClient client = new ArtifactLocalizerClient(cConf, new NoOpInternalAuthenticator());

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(TEMP_FOLDER.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    File newAppJarFile = new File(TEMP_FOLDER.newFolder(),
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

    //Make sure the two paths arent the same and the old directory still exists
    Assert.assertNotEquals(unpackedDir, newUnpackDir);
    validateUnpackDir(newUnpackDir);
    validateUnpackDir(unpackedDir);

    // Assert that the old cache directory is deleted after we run cleanup
    this.localizerService.forceCleanup();
    Assert.assertFalse(unpackedDir.exists());
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

  @Test
  public void testArtifact() throws Exception {
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    ArtifactLocalizerClient client = new ArtifactLocalizerClient(cConf, new NoOpInternalAuthenticator());

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

    File artifactPath = client.getArtifactLocation(artifactId.toEntityId());

    // Make sure the artifact was actually cached
    Assert.assertTrue(artifactPath.exists());
    Assert.assertTrue(artifactPath.isFile());

    // Call the sidecar again and make sure the same path was returned
    File sameArtifactPath = client.getArtifactLocation(artifactId.toEntityId());
    Assert.assertEquals(artifactPath, sameArtifactPath);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);

    // This sleep is needed to delay the file copy so that the lastModified time on the file is different
    Thread.sleep(1000);

    // Wait a bit before recreating the artifact to make sure the last modified time is different
    Files.copy(appJarFile, newAppJarFile);
    artifactRepository.addArtifact(artifactId, newAppJarFile);

    File newArtifactPath = client.getArtifactLocation(artifactId.toEntityId());

    //Make sure the two paths arent the same and that the old one is gone
    Assert.assertNotEquals(artifactPath, newArtifactPath);
    Assert.assertTrue(newArtifactPath.exists());
  }

  private class NoOpArtifactManager implements ArtifactManager {
    @Override
    public List<ArtifactInfo> listArtifacts() throws IOException, AccessException {
      return Collections.emptyList();
    }

    @Override
    public List<ArtifactInfo> listArtifacts(String namespace) throws IOException, AccessException {
      return Collections.emptyList();
    }

    @Override
    public CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
                                                  @Nullable ClassLoader parentClassLoader)
      throws IOException, AccessException {
      return null;
    }

    @Override
    public CloseableClassLoader createClassLoader(String namespace, ArtifactInfo artifactInfo,
                                                  @Nullable ClassLoader parentClassLoader)
      throws IOException, AccessException {
      return null;
    }
  }
}
