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
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;

/**
 * Test for Artifact Cache service.
 */
public class ArtifactCacheServiceTest extends AppFabricTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private CConfiguration cConf;
  private TetheringStore tetheringStore;
  private ArtifactCacheService artifactCacheService;

  private CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.ArtifactCache.ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.setInt(Constants.ArtifactCache.PORT, 11030);
    cConf.setInt(Constants.ArtifactCache.CACHE_CLEANUP_INTERVAL_MIN, 60);
    cConf.set(Constants.ArtifactCache.LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.ArtifactCache.BOSS_THREADS, 1);
    cConf.setInt(Constants.ArtifactCache.WORKER_THREADS, 5);
    return cConf;
  }

  @Before
  public void setUp() throws Exception {
    cConf = createCConf();
    tetheringStore = getInjector().getInstance(TetheringStore.class);
    ArtifactCache artifactCache = new ArtifactCache(cConf, tetheringStore);
    artifactCacheService = new ArtifactCacheService(cConf, artifactCache);
    artifactCacheService.startAndWait();
    getInjector().getInstance(ArtifactRepository.class).clear(NamespaceId.DEFAULT);
  }

  @After
  public void tearDown() throws Exception {
    artifactCacheService.stopAndWait();
  }

  private byte[] getArtifactBytes(ArtifactId artifactId) throws PeerAlreadyExistsException, IOException {
    String urlPath = String.format("/peers/peer/namespaces/%s/artifacts/%s/versions/%s",
                                   artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion());
    URL url;
    try {
      url = new URI(String.format("http://%s:%d/v3Internal/%s",
                                  cConf.get(Constants.ArtifactCache.ADDRESS),
                                  cConf.getInt(Constants.ArtifactCache.PORT),
                                  urlPath))
        .toURL();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    return httpResponse.getResponseBody();
  }

  private void addPeer() throws PeerAlreadyExistsException, IOException {
    NamespaceAllocation namespaceAllocation = new NamespaceAllocation("default", null,
                                                                      null);
    PeerMetadata peerMetadata = new PeerMetadata(Collections.singletonList(namespaceAllocation),
                                                 Collections.emptyMap());
    PeerInfo peerInfo = new PeerInfo("peer", getEndPoint("").toString(),
                                     TetheringStatus.ACCEPTED, peerMetadata);
    tetheringStore.addPeer(peerInfo);
  }

  @Test
  public void testFetchArtifact() throws Exception {
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    artifactRepository.addArtifact(artifactId, appJarFile);
    addPeer();

    byte[] artifact = getArtifactBytes(artifactId.toEntityId());
    byte[] expectedBytes = IOUtils.toByteArray(appJar.getInputStream());
    Assert.assertArrayEquals(artifact, expectedBytes);

    appJar.delete();
  }
}
