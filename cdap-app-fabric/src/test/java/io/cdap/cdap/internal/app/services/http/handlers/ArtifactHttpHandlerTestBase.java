/*
 * Copyright © 2018-2020 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandler;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.commons.io.FileUtils;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * TestBase for {@link ArtifactHttpHandler}. Contains common setup and other common methods
 */
public abstract class ArtifactHttpHandlerTestBase extends AppFabricTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static String systemArtifactsDir;
  private static ArtifactRepository artifactRepository;
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();

  @BeforeClass
  public static void setup() {
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    systemArtifactsDir = getInjector().getInstance(CConfiguration.class).get(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR);
    DiscoveryServiceClient discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
      () -> discoveryClient.discover(Constants.Service.METADATA_SERVICE));
    Discoverable discoverable = endpointStrategy.pick(1, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    String host = discoverable.getSocketAddress().getHostName();
    int port = discoverable.getSocketAddress().getPort();
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(host)
      .setPort(port)
      .setSSLEnabled(URIScheme.HTTPS.isMatch(discoverable))
      .build();
    ClientConfig clientConfig = ClientConfig.builder()
      .setVerifySSLCert(false)
      .setConnectionConfig(connectionConfig).build();
  }

  @After
  public void wipeData() throws Exception {
    artifactRepository.clear(NamespaceId.DEFAULT);
    artifactRepository.clear(NamespaceId.SYSTEM);
    // Clean up system artifact dir as well so they don't get auto reloaded by system artifact loader
    // which scans system artifact directory.
    FileUtils.cleanDirectory(new File(systemArtifactsDir));
  }

  /**
   * Add {@link AllProgramsApp} as system artifact with the given {@link ArtifactId}
   * which can be used as parent artifact for testing purpose
   */
  protected void addAppAsSystemArtifacts(ArtifactId artifactId) throws Exception {
    File destination = new File(String.format("%s/%s-%s.jar",
                                              systemArtifactsDir, artifactId.getArtifact(), artifactId.getVersion()));
    buildAppArtifact(AllProgramsApp.class, new Manifest(), destination);
    artifactRepository.addSystemArtifacts();
  }

  /**
   * Add {@link AllProgramsApp} as user artifact with the given {@link ArtifactId}
   */
  protected void addAppAsUserArtifacts(ArtifactId artifactId) throws Exception {
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    String namespaceDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    File namespaceBase = new File(localDataDir, namespaceDir);
    File destination = new File(new File(namespaceBase, artifactId.getNamespace()),
                                String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    buildAppArtifact(AllProgramsApp.class, new Manifest(), destination);
    artifactRepository.addArtifact(new Id.Artifact(
                                     new Id.Namespace(artifactId.getNamespace()),
                                     artifactId.getArtifact(),
                                     new ArtifactVersion(artifactId.getVersion())),
                                   destination);
  }


  /**
   * Get {@link ArtifactInfo} of all artifacts in the given namespace. Artifacts from system namespace should be
   * included.
   */
  List<ArtifactInfo> listArtifactsInternal(String namespace) throws IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts",
                                             Constants.Gateway.INTERNAL_API_VERSION_3, namespace)).toURL();
    return getResults(endpoint, ARTIFACT_INFO_LIST_TYPE);
  }

  /**
   * Get the location path of the given artifact.
   */
  String getArtifactLocationPath(ArtifactId artifactId) throws ArtifactNotFoundException, IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s/location",
                                             Constants.Gateway.INTERNAL_API_VERSION_3,
                                             artifactId.getNamespace(),
                                             artifactId.getArtifact(),
                                             artifactId.getVersion())).toURL();
    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(endpoint).build(),
                                                 new DefaultHttpRequestConfig(false));

    int responseCode = httpResponse.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ArtifactNotFoundException(artifactId);
    }
    Assert.assertEquals(HttpURLConnection.HTTP_OK, responseCode);
    return httpResponse.getResponseBodyAsString();
  }

  /**
   * @return {@link ArtifactInfo} of the given artifactId
   */
  protected ArtifactInfo getArtifactInfo(ArtifactId artifactId) throws IOException {
    // get /artifacts/{name}/versions/{version}
    return getArtifactInfo(artifactId, null);
  }

  /**
   * @return {@link ArtifactInfo} of the given artifactId in the given scope
   */
  protected ArtifactInfo getArtifactInfo(ArtifactId artifactId, ArtifactScope scope) throws IOException {
    // get /artifacts/{name}/versions/{version}?scope={scope}
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s%s",
                                             Constants.Gateway.API_VERSION_3, artifactId.getNamespace(),
                                             artifactId.getArtifact(), artifactId.getVersion(),
                                             getScopeQuery(scope))).toURL();

    return getResults(endpoint, ArtifactInfo.class);
  }

  /**
   * @return the contents from doing a get on the given url as the given type, or null if a 404 is returned
   */
  @Nullable
  <T> T getResults(URL endpoint, Type type) throws IOException {
    HttpResponse response = HttpRequests.execute(HttpRequest.get(endpoint).build(),
                                                 new DefaultHttpRequestConfig(false));

    int responseCode = response.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
      return null;
    }
    Assert.assertEquals(HttpURLConnection.HTTP_OK, responseCode);

    return GSON.fromJson(response.getResponseBodyAsString(), type);
  }

  /**
   * @return the given scope as a query parameter string or empty string if the scope is null
   */
  String getScopeQuery(ArtifactScope scope) {
    return (scope == null) ? ("") : ("?scope=" + scope.name());
  }

  /**
   * Get the {@link ArtifactDetail} of the given artifact directly from {@link ArtifactRepository}. This is typically
   * used as expected value to verify the info fetched via REST API calls.
   */
  ArtifactDetail getArtifactDetailFromRepository(Id.Artifact artifactId) throws Exception {
    return artifactRepository.getArtifact(artifactId);
  }
}
