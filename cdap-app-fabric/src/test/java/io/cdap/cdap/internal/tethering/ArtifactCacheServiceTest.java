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

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for Artifact Cache service.
 */
public class ArtifactCacheServiceTest extends AppFabricTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final String PEER_NAME = "peer";

  private CConfiguration cConf;
  private TetheringStore tetheringStore;
  private ArtifactCacheService artifactCacheService;
  private ArtifactRepository artifactRepository;
  private Id.Artifact artifactId;
  private Location appJar;

  private CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.ArtifactCache.ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.setInt(Constants.ArtifactCache.PORT, 11030);
    cConf.set(Constants.ArtifactCache.LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return cConf;
  }

  @Before
  public void setUp() throws Exception {
    cConf = createCConf();
    tetheringStore = getInjector().getInstance(TetheringStore.class);
    ArtifactCache artifactCache = new ArtifactCache(cConf);
    DiscoveryService discoveryService = getInjector().getInstance(DiscoveryService.class);
    artifactCacheService = new ArtifactCacheService(
      cConf, artifactCache, tetheringStore, null, discoveryService,
      new CommonNettyHttpServiceFactory(cConf, new NoOpMetricsCollectionService(), auditLogContexts -> {}));
    artifactCacheService.startAndWait();
    getInjector().getInstance(ArtifactRepository.class).clear(NamespaceId.DEFAULT);
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    artifactRepository.addArtifact(artifactId, appJarFile);
    addPeer();
  }

  @After
  public void tearDown() throws Exception {
    artifactCacheService.stopAndWait();
    artifactRepository.deleteArtifact(artifactId);
    appJar.delete();
    deletePeer();
  }

  @Test
  public void testFetchArtifact() throws Exception {
    Assert.assertArrayEquals(IOUtils.toByteArray(appJar.getInputStream()),
                                                 IOUtils.toByteArray(artifactRepository.newInputStream(artifactId)));
  }

  @Test(expected = NotFoundException.class)
  public void testFetchArtifactUnknownPeer() throws Exception {
    ArtifactRepository artifactRepository = getArtifactRepository("unknownpeer");
    artifactRepository.getArtifact(artifactId);
  }

  @Test(expected = NotFoundException.class)
  public void testArtifactNotFound() throws Exception {
    Id.Artifact notFoundArtifact = Id.Artifact.from(Id.Namespace.DEFAULT, "other-task", "2.0.0-SNAPSHOT");
    ArtifactRepository artifactRepository = getArtifactRepository();
    artifactRepository.getArtifact(notFoundArtifact);
  }

  @Test
  public void testGetArtifactDetail() throws Exception {
    ArtifactRepository artifactRepository = getArtifactRepository();
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    Assert.assertEquals(artifactId.toArtifactId(), artifactDetail.getDescriptor().getArtifactId());
    Assert.assertEquals(artifactId.getNamespace().getId(), artifactDetail.getDescriptor().getNamespace());
  }

  private ArtifactRepository getArtifactRepository() {
    return getArtifactRepository(PEER_NAME);
  }

  private ArtifactRepository getArtifactRepository(String peer) {
    RemoteClientFactory remoteClientFactory = getInjector().getInstance(RemoteClientFactory.class);
    String basePath = String.format("%s/peers/%s", Constants.Gateway.INTERNAL_API_VERSION_3, peer);
    RemoteClient remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.ARTIFACT_CACHE_SERVICE,
      RemoteClientFactory.NO_VERIFY_HTTP_REQUEST_CONFIG,
      basePath);
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    RemoteArtifactRepositoryReader artifactRepositoryReader = new RemoteArtifactRepositoryReader(
      locationFactory, remoteClient);
    return new RemoteArtifactRepository(cConf, artifactRepositoryReader);
  }

  private void addPeer() throws PeerAlreadyExistsException, IOException {
    NamespaceAllocation namespaceAllocation = new NamespaceAllocation("default", null,
                                                                      null);
    PeerMetadata peerMetadata = new PeerMetadata(Collections.singletonList(namespaceAllocation),
                                                 Collections.emptyMap(), null);
    PeerInfo peerInfo = new PeerInfo(PEER_NAME, getEndPoint("").toString(),
                                     TetheringStatus.ACCEPTED, peerMetadata, System.currentTimeMillis());
    tetheringStore.addPeer(peerInfo);
  }

  private void deletePeer() throws PeerNotFoundException, IOException {
    tetheringStore.deletePeer(PEER_NAME);
  }
}
