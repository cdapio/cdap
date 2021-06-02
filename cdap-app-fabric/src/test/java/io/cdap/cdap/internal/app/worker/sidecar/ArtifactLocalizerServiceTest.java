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
import com.google.gson.Gson;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemotePluginFinder;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.RemoteWorkerPluginFinder;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Unit test for {@link ArtifactLocalizerService}.
 */
public class ArtifactLocalizerServiceTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerServiceTest.class);
  private static final Gson GSON = new Gson();

  private ArtifactLocalizerService localizerService;

  private CConfiguration createCConf(int port) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, port);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    return cConf;
  }

  private SConfiguration createSConf() {
    SConfiguration sConf = SConfiguration.create();
    return sConf;
  }

  private ArtifactLocalizerService setupFileLocalizerService(int port) throws IOException {
    CConfiguration cConf = createCConf(port);
    SConfiguration sConf = createSConf();
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);
    RemotePluginFinder remotePluginFinder = getInjector().getInstance(RemotePluginFinder.class);
    ArtifactLocalizerService artifactLocalizerService =
      new ArtifactLocalizerService(cConf, sConf, new InMemoryDiscoveryService(),
                                   new ArtifactLocalizer(cConf, discoveryClient,
                                                         remotePluginFinder, tmpFolder.newFolder()),
                                   new LocalLocationFactory());
    // start the service
    artifactLocalizerService.startAndWait();

    return artifactLocalizerService;
  }

  @Before
  public void setUp() throws Exception {
    this.localizerService = setupFileLocalizerService(10001);
  }

  @After
  public void tearDown() throws Exception {
    this.localizerService.shutDown();
  }

  @Test
  public void testArtifact() throws Exception {

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    RemoteWorkerPluginFinder pluginFinder = getInjector().getInstance(RemoteWorkerPluginFinder.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    InetSocketAddress addr = localizerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));
    String url = String.format("/v3Internal/worker/artifact/namespaces/%s/artifacts/%s/versions/%s", artifactId.getNamespace().getId(), artifactId.getName(), artifactId.getVersion());
    HttpRequest request = HttpRequest.get(uri.resolve(url).toURL()).build();
    HttpResponse response = HttpRequests.execute(request);


    artifactRepository.addArtifact(artifactId, appJarFile);

    pluginFinder.getArtifactLocation(artifactId.toEntityId());
  }

  @Test
  public void testLocalize() throws Exception {

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    artifactRepository.addArtifact(artifactId, appJarFile);

    //Send a valid get request
    InetSocketAddress addr = localizerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));
    String url = "/v3Internal/worker/localize" + appJarFile.toString();
    HttpRequest request = HttpRequest.get(uri.resolve(url).toURL()).build();
    HttpResponse response = HttpRequests.execute(request);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    String artifactPath = response.getResponseBodyAsString();

    // Make sure the artifact was actually cached
    Assert.assertTrue(Paths.get(artifactPath).toFile().exists());

    //Send the request again and make sure the same path is returned
    HttpResponse secondResponse = HttpRequests.execute(request);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, secondResponse.getResponseCode());
    String secondArtifact = secondResponse.getResponseBodyAsString();
    Assert.assertEquals(artifactPath, secondArtifact);
  }

  @Test
  public void testUnpack() throws Exception {
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    artifactRepository.addArtifact(artifactId, appJarFile);

    // Get the unpacked artifact path
    InetSocketAddress addr = localizerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));
    String url = "/v3Internal/worker/unpack" + appJarFile.toString();
    HttpRequest request = HttpRequest.get(uri.resolve(url).toURL()).build();
    HttpResponse response = HttpRequests.execute(request);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    String unpackedDir = response.getResponseBodyAsString();

    // Make sure the artifact was actually cached
    File unpackedFile = Paths.get(unpackedDir).toFile();

    // Make sure the directory exists
    Assert.assertTrue(unpackedFile.exists());
    Assert.assertTrue(unpackedFile.isDirectory());

    // Make sure theres multiple files in the directory and one of them is a manifest
    String[] fileNames = unpackedFile.list();
    Assert.assertTrue(fileNames.length > 1);
    Assert.assertTrue(Arrays.stream(fileNames).anyMatch(s -> s.equals("META-INF")));
  }
}
