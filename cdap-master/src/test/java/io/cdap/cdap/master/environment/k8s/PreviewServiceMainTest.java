/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.master.environment.app.PreviewTestApp;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link AppFabricServiceMain}.
 */
public class PreviewServiceMainTest extends MasterServiceMainTestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void testPreviewService() throws Exception {

    // Deploy the app artifact
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, PreviewTestApp.class);

    String artifactName = PreviewTestApp.class.getSimpleName();
    String artifactVersion = "1.0.0-SNAPSHOT";

    HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0, false);
    URL url = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/artifacts/%s", artifactName)).toURL();
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(url)
        .withBody((ContentProvider<? extends InputStream>) appJar::getInputStream)
        .addHeader("Artifact-Version", artifactVersion)
        .build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // have to stop AppFabric so that Preview can share the same leveldb table
    AppFabricServiceMain appFabricServiceMain = getServiceMainInstance(AppFabricServiceMain.class);
    appFabricServiceMain.stop();
    Injector injector = appFabricServiceMain.getInjector();
    injector.getInstance(LevelDBTableService.class).close();

    // start preview service with the same data dir as app-fabric, so that the artifact info is still there.
    runMain(injector.getInstance(CConfiguration.class), injector.getInstance(SConfiguration.class),
            PreviewServiceMain.class, AppFabricServiceMain.class.getSimpleName());

    // create a preview run
    url = getRouterBaseURI().resolve("/v3/namespaces/default/previews").toURL();
    ArtifactSummary artifactSummary = new ArtifactSummary(artifactName, artifactVersion);
    PreviewConfig previewConfig = new PreviewConfig(PreviewTestApp.TestWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.emptyMap(), 2);
    AppRequest appRequest = new AppRequest<>(artifactSummary, null, previewConfig);
    response = HttpRequests.execute(HttpRequest.post(url).withBody(GSON.toJson(appRequest)).build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    ApplicationId previewId = GSON.fromJson(response.getResponseBodyAsString(), ApplicationId.class);

    URL statusUrl = getRouterBaseURI()
      .resolve(String.format("/v3/namespaces/default/previews/%s/status", previewId.getApplication())).toURL();
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, () -> {
      HttpResponse statusResponse = HttpRequests.execute(HttpRequest.get(statusUrl).build(), requestConfig);
      if (statusResponse.getResponseCode() != 200) {
        return null;
      }
      PreviewStatus previewStatus = GSON.fromJson(statusResponse.getResponseBodyAsString(), PreviewStatus.class);
      return previewStatus.getStatus();
    }, 2, TimeUnit.MINUTES);

    url = getRouterBaseURI()
      .resolve(String.format("/v3/namespaces/default/previews/%s/tracers/%s",
                             previewId.getApplication(), PreviewTestApp.TRACER_NAME)).toURL();
    response = HttpRequests.execute(HttpRequest.get(url).build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Map<String, List<String>> tracerData = GSON.fromJson(response.getResponseBodyAsString(),
                                                         new TypeToken<Map<String, List<String>>>() { }.getType());
    Assert.assertEquals(Collections.singletonMap(PreviewTestApp.TRACER_KEY,
                                                 Collections.singletonList(PreviewTestApp.TRACER_VAL)),
                        tracerData);
  }
}
