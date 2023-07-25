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
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.common.http.HttpMethod;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class ArtifactLocalizerTest extends AppFabricTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private ArtifactLocalizer createArtifactLocalizer(ArtifactManager mockManager,
      RemoteClient mockClient)
      throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(
        DiscoveryServiceClient.class);

    RemoteClientFactory remoteClientFactory = Mockito.mock(RemoteClientFactory.class);
    Mockito.when(remoteClientFactory.createRemoteClient(Mockito.anyString(), Mockito.any(),
            Mockito.anyString()))
        .thenReturn(mockClient);
    return new ArtifactLocalizer(cConf, remoteClientFactory,
        (namespaceId, retryStrategy) -> mockManager);
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

}
