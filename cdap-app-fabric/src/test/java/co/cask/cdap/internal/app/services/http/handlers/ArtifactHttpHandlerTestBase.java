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
package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.gateway.handlers.ArtifactHttpHandler;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
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
import javax.ws.rs.HttpMethod;

/**
 * TestBase for {@link ArtifactHttpHandler}. Contains common setup and other common methods
 */
public abstract class ArtifactHttpHandlerTestBase extends AppFabricTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static String systemArtifactsDir;
  static ArtifactRepository artifactRepository;
  static MetadataClient metadataClient;

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
    ConnectionConfig connectionConfig = ConnectionConfig.builder().setHostname(host).setPort(port).build();
    ClientConfig clientConfig = ClientConfig.builder().setConnectionConfig(connectionConfig).build();
    metadataClient = new MetadataClient(clientConfig);
  }

  @After
  public void wipeData() throws Exception {
    artifactRepository.clear(NamespaceId.DEFAULT);
    artifactRepository.clear(NamespaceId.SYSTEM);
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
    HttpURLConnection urlConn = (HttpURLConnection) endpoint.openConnection();
    urlConn.setRequestMethod(HttpMethod.GET);

    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpResponseStatus.NOT_FOUND.code()) {
      return null;
    }
    Assert.assertEquals(HttpResponseStatus.OK.code(), responseCode);

    String responseStr = new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8);
    urlConn.disconnect();
    return GSON.fromJson(responseStr, type);
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
