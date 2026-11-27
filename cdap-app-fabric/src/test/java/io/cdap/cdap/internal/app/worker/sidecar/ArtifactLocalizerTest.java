/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class ArtifactLocalizerTest extends AppFabricTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private ArtifactLocalizer createArtifactLocalizer(File dataDir, ArtifactManager mockManager,
      RemoteClient mockClient) {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());

    RemoteClientFactory remoteClientFactory = Mockito.mock(RemoteClientFactory.class);
    Mockito.when(remoteClientFactory.createRemoteClient(Mockito.anyString(), Mockito.any(),
            Mockito.anyString()))
        .thenReturn(mockClient);
    return new ArtifactLocalizer(cConf, remoteClientFactory,
        (namespaceId, retryStrategy) -> mockManager);
  }

  private ArtifactLocalizer createArtifactLocalizer(ArtifactManager mockManager,
      RemoteClient mockClient)
      throws IOException {
    return createArtifactLocalizer(TEMP_FOLDER.newFolder(), mockManager, mockClient);
  }

  @Test
  public void testPreloadArtifacts() throws IOException, ArtifactNotFoundException {
    ArtifactInfo dataPipeline1 = new ArtifactInfo("cdap-data-pipeline", "6.8", null, null, null);
    ArtifactInfo dataPipeline2 = new ArtifactInfo("cdap-data-pipeline", "6.9", null, null, null);
    ArtifactInfo dataPipeline3 = new ArtifactInfo("cdap-data-pipeline", "6.10", null, null, null);
    ArtifactInfo wrangler1 = new ArtifactInfo("wrangler", "4.8", null, null, null);
    ArtifactInfo wrangler2 = new ArtifactInfo("wrangler", "4.9", null, null, null);
    ArtifactInfo wrangler3 = new ArtifactInfo("wrangler", "4.10", null, null, null);
    List<ArtifactInfo> infos = Arrays.asList(dataPipeline1, dataPipeline2, dataPipeline3, wrangler1,
        wrangler2, wrangler3);

    ArtifactManager mockManager = Mockito.mock(ArtifactManager.class);
    Mockito.when(mockManager.listArtifacts()).thenReturn(infos);
    RemoteClient mockClient = Mockito.mock(RemoteClient.class);

    ArtifactLocalizer localizer = createArtifactLocalizer(mockManager, mockClient);
    localizer.preloadArtifacts(new HashSet<>(Arrays.asList("cdap-data-pipeline", "wrangler")), 2);

    String downloadUrlFormat = "namespaces/default/artifacts/%s/versions/%s/download?scope=SYSTEM";

    Mockito.verify(mockClient)
        .openConnection(HttpMethod.GET,
            String.format(downloadUrlFormat, "cdap-data-pipeline", "6.10"));
    Mockito.verify(mockClient)
        .openConnection(HttpMethod.GET,
            String.format(downloadUrlFormat, "cdap-data-pipeline", "6.9"));
    Mockito.verify(mockClient)
        .openConnection(HttpMethod.GET, String.format(downloadUrlFormat, "wrangler", "4.10"));
    Mockito.verify(mockClient)
        .openConnection(HttpMethod.GET, String.format(downloadUrlFormat, "wrangler", "4.9"));
    Mockito.verify(mockClient, Mockito.times(4)).openConnection(Mockito.any(), Mockito.anyString());
  }

  @Test
  public void testConcurrentGetAndUnpackArtifact() throws Exception {
    File dataDir = TEMP_FOLDER.newFolder();
    ArtifactId artifactId = NamespaceId.SYSTEM.artifact("cdap-data-pipeline", "6.12.0");
    long timestamp = 1795466453000L;

    // Pre-populate cached artifact JAR on disk just as preloadArtifacts() does at sidecar startup.
    Path cachedJarDir = Paths.get(dataDir.getAbsolutePath(), "artifacts",
        artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion());
    DirUtils.mkdirs(cachedJarDir.toFile());
    File cachedJar = cachedJarDir.resolve(timestamp + ".jar").toFile();

    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location bundleJar = AppJarHelper.createDeploymentJar(
        locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    Locations.linkOrCopy(bundleJar, cachedJar);
    bundleJar.delete();

    RemoteClient mockClient = Mockito.mock(RemoteClient.class);
    Mockito.when(mockClient.openConnection(Mockito.eq(HttpMethod.GET), Mockito.anyString()))
        .thenAnswer(invocation -> {
          HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
          Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_NOT_MODIFIED);
          return conn;
        });

    ArtifactLocalizer localizer = createArtifactLocalizer(
        dataDir, Mockito.mock(ArtifactManager.class), mockClient);

    int threadCount = 16;
    CyclicBarrier startBarrier = new CyclicBarrier(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      List<Future<File>> futures = new ArrayList<>(threadCount);
      for (int i = 0; i < threadCount; i++) {
        futures.add(executor.submit(() -> {
          startBarrier.await(10, TimeUnit.SECONDS);
          return localizer.getAndUnpackArtifact(artifactId);
        }));
      }

      File expectedUnpackDir = Paths.get(dataDir.getAbsolutePath(), "unpacked",
          artifactId.getNamespace(), artifactId.getArtifact(), artifactId.getVersion(),
          String.valueOf(timestamp)).toFile();
      for (Future<File> future : futures) {
        File result = future.get(30, TimeUnit.SECONDS);
        Assert.assertEquals(expectedUnpackDir, result);
      }

      Assert.assertTrue(expectedUnpackDir.isDirectory());
      List<String> unpackedEntries = DirUtils.list(expectedUnpackDir);
      Assert.assertTrue(unpackedEntries.contains("META-INF"));
      // Verify no leftover temp unpack directories remain alongside the timestamp directory.
      Assert.assertEquals(1, DirUtils.list(expectedUnpackDir.getParentFile()).size());
    } finally {
      executor.shutdownNow();
    }
  }

}
