/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandler;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
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
  private static DiscoveryServiceClient discoveryClient;
  private static LocationFactory locationFactory;

  @BeforeClass
  public static void setup() {
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    systemArtifactsDir = getInjector().getInstance(CConfiguration.class).get(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    discoveryClient = getInjector().getInstance(DiscoveryServiceClient.class);
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
  }

  protected static DiscoveryServiceClient getDiscoveryClient() {
    return discoveryClient;
  }

  protected static LocationFactory getLocationFactory() {
    return locationFactory;
  }

  /**
   * Adds {@link AllProgramsApp} as system artifact which can be used as parent artifact for testing purpose
   * @throws Exception
   */
  void addAppAsSystemArtifacts() throws Exception {
    File destination = new File(systemArtifactsDir + "/app-1.0.0.jar");
    buildAppArtifact(AllProgramsApp.class, new Manifest(), destination);
    artifactRepository.addSystemArtifacts();
  }

  /**
   * @return {@link ArtifactInfo} of the given artifactId
   */
  ArtifactInfo getArtifact(ArtifactId artifactId) throws URISyntaxException, IOException {
    // get /artifacts/{name}/versions/{version}
    return getArtifact(artifactId, null);
  }

  /**
   * @return {@link ArtifactInfo} of the given artifactId in the given scope
   */
  ArtifactInfo getArtifact(ArtifactId artifactId, ArtifactScope scope) throws URISyntaxException, IOException {
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
   * Deletes all the jar in the systemArtifactDir
   */
  void cleanupSystemArtifactsDirectory() {
    File dir = new File(systemArtifactsDir);
    for (File jarFile : DirUtils.listFiles(dir, "jar")) {
      jarFile.delete();
    }
  }
}
