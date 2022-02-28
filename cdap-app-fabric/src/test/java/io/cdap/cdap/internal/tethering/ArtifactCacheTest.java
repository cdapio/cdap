/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerCleaner;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Test for Artifact Cache.
 */
public class ArtifactCacheTest extends AppFabricTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testArtifactCache() throws Exception {
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));

    Locations.linkOrCopy(appJar, appJarFile);
    artifactRepository.addArtifact(artifactId, appJarFile);

    CConfiguration cConf = CConfiguration.create();
    String cacheDir = tmpFolder.toString();
    cConf.set(Constants.ArtifactCache.LOCAL_DATA_DIR, cacheDir);
    TetheringStore store = getInjector().getInstance(TetheringStore.class);
    ArtifactCache cache = new ArtifactCache(cConf);
    // Add a couple of tethered peers
    addPeer(store, "peer1");
    addPeer(store, "peer2");

    RemoteClientFactory factory = new RemoteClientFactory(new NoOpDiscoveryServiceClient(getEndPoint("").toString()),
                                                          new NoOpInternalAuthenticator());
    HttpRequestConfig config = new DefaultHttpRequestConfig(true);
    RemoteClient remoteClient = factory.createRemoteClient("", config,
                                                           Constants.Gateway.INTERNAL_API_VERSION_3);
    // Get artifact from first peer
    File peer1ArtifactPath = cache.getArtifact(artifactId.toEntityId(), "peer1", remoteClient);
    // Get the artifact again. The same path was returned
    Assert.assertEquals(peer1ArtifactPath, cache.getArtifact(artifactId.toEntityId(), "peer1", remoteClient));

    // Get artifact from another peer. It should be cached in a different path
    File peer2ArtifactPath = cache.getArtifact(artifactId.toEntityId(), "peer2", remoteClient);
    Assert.assertNotEquals(peer1ArtifactPath, peer2ArtifactPath);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);
    // This sleep is needed to delay the file copy so that the lastModified time on the file is different
    Thread.sleep(1000);
    artifactRepository.addArtifact(artifactId, appJarFile);
    // Artifact should be cached in a different path
    File newPeer1ArtifactPath = cache.getArtifact(artifactId.toEntityId(), "peer1", remoteClient);
    Assert.assertNotEquals(peer1ArtifactPath, newPeer1ArtifactPath);

    // Run the artifact cleaner
    ArtifactLocalizerCleaner cleaner = new ArtifactLocalizerCleaner(Paths.get(cacheDir).resolve("peers"), 1);
    cleaner.run();
    // Older artifact should been deleted
    Assert.assertFalse(peer1ArtifactPath.exists());
    // Latest artifact should still be cached
    Assert.assertTrue(newPeer1ArtifactPath.exists());
  }

  private void addPeer(TetheringStore tetheringStore, String peerName) throws PeerAlreadyExistsException, IOException {
    NamespaceAllocation namespaceAllocation = new NamespaceAllocation("default", null,
                                                                      null);
    PeerMetadata peerMetadata = new PeerMetadata(Collections.singletonList(namespaceAllocation),
                                                 Collections.emptyMap(), null);
    PeerInfo peerInfo = new PeerInfo(peerName, getEndPoint("").toString(),
                                     TetheringStatus.ACCEPTED, peerMetadata, System.currentTimeMillis());
    tetheringStore.addPeer(peerInfo);
  }
}
