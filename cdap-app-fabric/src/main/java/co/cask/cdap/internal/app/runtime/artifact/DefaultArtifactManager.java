/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.artifact;


import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.PluginClassDeserializer;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation for {@link co.cask.cdap.api.artifact.ArtifactManager}
 * communicating with {@link co.cask.cdap.gateway.handlers.ArtifactHttpHandler} and returning artifact info.
 */
public class DefaultArtifactManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultArtifactManager.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();

  private final DiscoveryServiceClient discoveryServiceClient;
  private final File tmpDir;
  private final ClassLoader bootstrapClassLoader;
  private final LocationFactory locationFactory;

  @Inject
  public DefaultArtifactManager(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                                LocationFactory locationFactory) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.locationFactory = locationFactory;
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    // There is no reliable way to get bootstrap ClassLoader from Java (System.class.getClassLoader() may return null).
    // A URLClassLoader with no URLs and with a null parent will load class from bootstrap ClassLoader only.
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  /**
   * For the specified namespace, return the available artifacts in the namespace
   * @param namespaceId namespace
   * @return {@link List<ArtifactInfo>}
   * @throws IOException If there are any exception while retrieving artifacts
   */
  public List<ArtifactInfo> listArtifacts(NamespaceId namespaceId) throws IOException {
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP);
    URL url = createURL(new RandomEndpointStrategy(discovered).pick(1, TimeUnit.SECONDS),
                        String.format("%s/artifact-internals/list/artifacts", namespaceId.getEntityName()));

    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(url).build());

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      throw new IOException("Could not list artifacts, endpoint not found");
    }

    if (isSuccessful(httpResponse.getResponseCode())) {
      List<ArtifactInfo> artifactInfoList =
        GSON.fromJson(httpResponse.getResponseBodyAsString(), new TypeToken<List<ArtifactInfo>>() { }.getType());
      return artifactInfoList;
    } else {
      throw new IOException(String.format("Exception while getting artifacts list %s",
                                          httpResponse.getResponseBodyAsString()));
    }
  }

  private boolean isSuccessful(int responseCode) {
    return responseCode == 200;
  }

  @Nullable
  private URL createURL(@Nullable Discoverable discoverable, String pathSuffix) throws IOException {
    if (discoverable == null) {
      return null;
    }
    InetSocketAddress address = discoverable.getSocketAddress();
    String scheme = Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload()) ?
      Constants.Security.SSL_URI_SCHEME : Constants.Security.URI_SCHEME;

    String path = String.format("%s%s:%d%s/namespaces/%s", scheme,
                                address.getHostName(), address.getPort(),
                                Constants.Gateway.API_VERSION_3, pathSuffix);

    return new URL(path);
  }

  /**
   * Create a class loader with artifact jar unpacked contents and parent for this classloader is the supplied
   * parentClassLoader, if that parent classloader is null, bootstrap classloader is used as parent.
   * This is a closeabled classloader, caller should call close when he is done using it, during close directory
   * cleanup will be performed.
   * @param namespaceId artifact namespace
   * @param artifactInfo artifact info whose artiact will be unpacked to create classloader
   * @param parentClassLoader  optional parent classloader, if null bootstrap classloader will be used
   * @return CloseableClassLoader call close on this CloseableClassLoader for cleanup
   * @throws IOException if artifact is not found or there were any error while getting artifact
   */
  public CloseableClassLoader createClassLoader(
    NamespaceId namespaceId, ArtifactInfo artifactInfo, @Nullable ClassLoader parentClassLoader) throws IOException {

    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP);
    String namespace = ArtifactScope.SYSTEM.equals(artifactInfo.getScope()) ?
      NamespaceId.SYSTEM.getNamespace() : namespaceId.getEntityName();
    URL url = createURL(new RandomEndpointStrategy(discovered).pick(1, TimeUnit.SECONDS),
                        String.format("%s/artifact-internals/artifacts/%s/versions/%s/location",
                                      namespace, artifactInfo.getName(), artifactInfo.getVersion()));

    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(url).build());

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      throw new IOException("Could not get artifact detail, endpoint not found");
    }

    if (isSuccessful(httpResponse.getResponseCode())) {
      String path = httpResponse.getResponseBodyAsString();
      Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
      if (!location.exists()) {
        throw new IOException(String.format("Artifact Location does not exist %s for artifact %s version %s",
                                            path, artifactInfo.getName(), artifactInfo.getVersion()));
      }
      final File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.unJar(location, unpackedDir);

      return new CloseableClassLoader(
        new DirectoryClassLoader(unpackedDir, parentClassLoader == null ? bootstrapClassLoader : parentClassLoader),
        new DeleteContents(unpackedDir));
    } else {
      throw new IOException(String.format("Exception while getting artifacts list %s",
                                          httpResponse.getResponseBodyAsString()));
    }
  }

  class DeleteContents implements Closeable {
    private final File directory;

    DeleteContents(File directory) {
      this.directory = directory;
    }
    @Override
    public void close() throws IOException {
      DirUtils.deleteDirectoryContents(directory);
    }
  }
}
