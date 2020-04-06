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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.master.environment.app.PreviewTestApp;
import io.cdap.cdap.master.environment.app.PreviewTestAppWithPlugin;
import io.cdap.cdap.master.environment.plugin.ConstantCallable;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * Unit test for {@link AppFabricServiceMain}.
 */
public class PreviewServiceMainTest extends MasterServiceMainTestBase {
  private static final Gson GSON = new Gson();

  private static final Type ARTIFACT_SUMMARY_LIST = new TypeToken<List<ArtifactSummary>>() { }.getType();

  @After
  public void after() throws IOException {
    deleteAllArtifact();
  }

  @Test
  public void testPreviewSimpleApp() throws Exception {
    // Build the app
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, PreviewTestApp.class);

    // Deploy the app
    String artifactName = PreviewTestApp.class.getSimpleName();
    String artifactVersion = "1.0.0-SNAPSHOT";
    deployArtifact(appJar, artifactName, artifactVersion);

    // Start the preview service main, which will use its own local datadir, thus should fetch artifact location
    // from appFabric when running a preview
    startService(PreviewServiceMain.class);

    // Run a preview
    ArtifactSummary artifactSummary = new ArtifactSummary(artifactName, artifactVersion);
    PreviewConfig previewConfig = new PreviewConfig(PreviewTestApp.TestWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.emptyMap(), 2);
    AppRequest appRequest = new AppRequest<>(artifactSummary, null, previewConfig);
    ApplicationId previewId = runPreview(appRequest);

    // Wait for preview to complete
    waitForPreview(previewId);

    // Verify the result of preview run
    URL url = getRouterBaseURI()
      .resolve(String.format("/v3/namespaces/default/previews/%s/tracers/%s",
                             previewId.getApplication(), PreviewTestApp.TRACER_NAME)).toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build(), getHttpRequestConfig());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Map<String, List<String>> tracerData = GSON.fromJson(response.getResponseBodyAsString(),
                                                         new TypeToken<Map<String, List<String>>>() {
                                                         }.getType());
    Assert.assertEquals(Collections.singletonMap(PreviewTestApp.TRACER_KEY,
                                                 Collections.singletonList(PreviewTestApp.TRACER_VAL)),
                        tracerData);

    // Stop the preview service main to
    stopService(PreviewServiceMain.class);

    // Clean up
    deleteArtfiact(artifactName, artifactVersion);
  }

  @Test
  public void testPreviewAppWithPlugin() throws Exception {
    // Build the app
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, PreviewTestAppWithPlugin.class);
    String appArtifactName = PreviewTestAppWithPlugin.class.getSimpleName() + "_artifact";
    String artifactVersion = "1.0.0-SNAPSHOT";

    // Deploy the app
    deployArtifact(appJar, appArtifactName, artifactVersion);
    HttpResponse response;

    // Build plugin artifact
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, ConstantCallable.class.getPackage().getName());
    Location pluginJar = PluginJarHelper.createPluginJar(locationFactory, manifest, ConstantCallable.class);

    // Deploy plug artifact
    String pluginArtifactName = ConstantCallable.class.getSimpleName() + "_artifact";
    URL pluginArtifactUrl =
      getRouterBaseURI().resolve(String.format("/v3/namespaces/default/artifacts/%s", pluginArtifactName)).toURL();
    response = HttpRequests.execute(
      HttpRequest
        .post(pluginArtifactUrl)
        .withBody((ContentProvider<? extends InputStream>) pluginJar::getInputStream)
        .addHeader("Artifact-Extends", String.format("%s[1.0.0-SNAPSHOT,10.0.0]", appArtifactName))
        .addHeader("Artifact-Version", artifactVersion)
        .build(), getHttpRequestConfig());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Start preview service
    startService(PreviewServiceMain.class);

    // Run a preview
    String expectedOutput = "output_value";
    ArtifactId appArtifactId = new ArtifactId(appArtifactName, new ArtifactVersion(artifactVersion),
                                              ArtifactScope.USER);
    ArtifactSummary artifactSummary = ArtifactSummary.from(appArtifactId);
    PreviewConfig previewConfig = new PreviewConfig(PreviewTestAppWithPlugin.TestWorkflow.NAME, ProgramType.WORKFLOW,
                                                    Collections.emptyMap(), 2);
    PreviewTestAppWithPlugin.Conf appConf =
      new PreviewTestAppWithPlugin.Conf(ConstantCallable.NAME,
                                        Collections.singletonMap("val", expectedOutput));
    AppRequest appRequest = new AppRequest<>(artifactSummary, appConf, previewConfig);
    ApplicationId previewId = runPreview(appRequest);

    // Wait for preview to complete
    waitForPreview(previewId);

    // Verify the result of preview run
    URL url = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/previews/%s/tracers/%s",
                                                       previewId.getApplication(), PreviewTestApp.TRACER_NAME)).toURL();
    response = HttpRequests.execute(HttpRequest.get(url).build(), getHttpRequestConfig());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Map<String, List<String>> tracerData = GSON.fromJson(response.getResponseBodyAsString(),
                                                         new TypeToken<Map<String, List<String>>>() {
                                                         }.getType());
    Assert.assertEquals(Collections.singletonMap(PreviewTestAppWithPlugin.TRACER_KEY,
                                                 Collections.singletonList(expectedOutput)),
                        tracerData);

    stopService(PreviewServiceMain.class);
  }

  /**
   * Wait for preview to complete with a deadline
   */
  private void waitForPreview(ApplicationId previewId) throws MalformedURLException,
    java.util.concurrent.TimeoutException, InterruptedException, java.util.concurrent.ExecutionException {
    URL statusUrl = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/previews/%s/status",
                                                             previewId.getApplication())).toURL();
    Tasks.waitFor(PreviewStatus.Status.COMPLETED, () -> {
      HttpResponse statusResponse = HttpRequests.execute(HttpRequest.get(statusUrl).build(), getHttpRequestConfig());
      if (statusResponse.getResponseCode() != 200) {
        return null;
      }
      PreviewStatus previewStatus = GSON.fromJson(statusResponse.getResponseBodyAsString(), PreviewStatus.class);
      return previewStatus.getStatus();
    }, 2, TimeUnit.MINUTES);
  }

  /**
   * Start a preview and return {@link ApplicationId}
   */
  private ApplicationId runPreview(AppRequest appRequest) throws IOException {
    URL url;
    url = getRouterBaseURI().resolve("/v3/namespaces/default/previews").toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).withBody(GSON.toJson(appRequest)).build(),
                                                 getHttpRequestConfig());
    Assert.assertEquals(response.getResponseBodyAsString(), HttpURLConnection.HTTP_OK, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), ApplicationId.class);
  }


  /**
   * Deploy the given application in default namespace
   */
  private void deployArtifact(Location artifactLocation, String artifactName, String artifactVersion)
    throws IOException {
    HttpRequestConfig requestConfig = getHttpRequestConfig();
    URL url = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/artifacts/%s", artifactName)).toURL();
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(url)
        .withBody((ContentProvider<? extends InputStream>) artifactLocation::getInputStream)
        .addHeader("Artifact-Version", artifactVersion)
        .build(),
      requestConfig);
    Assert.assertEquals(response.getResponseBodyAsString(), HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  /**
   * Delete the given artifact from default namespace
   */
  private void deleteArtfiact(String artifactName, String artifactVersion) throws IOException {
    HttpRequestConfig requestConfig = getHttpRequestConfig();
    URL url = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/artifacts/%s/versions/%s",
                                                       artifactName, artifactVersion)).toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.delete(url).build(), requestConfig);
    Assert.assertEquals(response.getResponseBodyAsString(), HttpURLConnection.HTTP_OK, response.getResponseCode());
  }

  /**
   * List all artifacts in the default namespaces and delete all of them.
   */
  private void deleteAllArtifact() throws IOException {
    HttpRequestConfig requestConfig = getHttpRequestConfig();
    URL url = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/artifacts")).toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build(), requestConfig);
    Assert.assertEquals(response.getResponseBodyAsString(), HttpURLConnection.HTTP_OK, response.getResponseCode());
    List<ArtifactSummary> summaryList = GSON.fromJson(response.getResponseBodyAsString(), ARTIFACT_SUMMARY_LIST);
    for (ArtifactSummary summary : summaryList) {
      url = getRouterBaseURI().resolve(String.format("/v3/namespaces/default/artifacts/%s/versions/%s",
                                                     summary.getName(), summary.getVersion())).toURL();
      response = HttpRequests.execute(HttpRequest.delete(url).build(), requestConfig);
      Assert.assertEquals(response.getResponseBodyAsString(), HttpURLConnection.HTTP_OK, response.getResponseCode());
    }
  }

  /**
   * Return a default {@link HttpRequestConfig} used for making REST calls
   */
  private HttpRequestConfig getHttpRequestConfig() {
    return new HttpRequestConfig(0, 0, false);
  }

}
