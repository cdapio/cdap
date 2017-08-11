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
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
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
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();
  private final File tmpDir;
  private final ClassLoader bootstrapClassLoader;
  private final LocationFactory locationFactory;
  private final AuthenticationContext authenticationContext;
  private final boolean authorizationEnabled;
  private final RemoteClient remoteClient;

  @Inject
  public DefaultArtifactManager(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                                LocationFactory locationFactory, AuthenticationContext authenticationContext) {
    this.locationFactory = locationFactory;
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    // There is no reliable way to get bootstrap ClassLoader from Java (System.class.getClassLoader() may return null).
    // A URLClassLoader with no URLs and with a null parent will load class from bootstrap ClassLoader only.
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
    this.authenticationContext = authenticationContext;
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         String.format("%s", Constants.Gateway.API_VERSION_3));
  }

  /**
   * For the specified namespace, return the available artifacts in the namespace and also the artifacts in system
   * namespace.
   *
   * If the app-fabric service is unavailable, it will be retried based on the passed in retry strategy.
   *
   * @param namespaceId namespace
   * @return {@link List<ArtifactInfo>}
   * @throws IOException If there are any exception while retrieving artifacts
   */
  public List<ArtifactInfo> listArtifacts(final NamespaceId namespaceId) throws IOException {
    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(HttpMethod.GET,
                                  String.format("namespaces/%s/artifact-internals/artifacts",
                                                namespaceId.getEntityName()));
    // add header if auth is enabled
    if (authorizationEnabled) {
      requestBuilder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }
    return getArtifactsList(requestBuilder.build());
  }

  /**
   * Create a class loader with artifact jar unpacked contents and parent for this classloader is the supplied
   * parentClassLoader, if that parent classloader is null, bootstrap classloader is used as parent.
   * This is a closeable classloader, caller should call close when he is done using it, during close directory
   * cleanup will be performed.
   *
   * @param namespaceId artifact namespace
   * @param artifactInfo artifact info whose artiact will be unpacked to create classloader
   * @param parentClassLoader  optional parent classloader, if null bootstrap classloader will be used
   * @return CloseableClassLoader call close on this CloseableClassLoader for cleanup
   * @throws IOException if artifact is not found or there were any error while getting artifact
   */
  public CloseableClassLoader createClassLoader(
    NamespaceId namespaceId, ArtifactInfo artifactInfo,
    @Nullable ClassLoader parentClassLoader) throws IOException {

    String namespace = ArtifactScope.SYSTEM.equals(artifactInfo.getScope()) ?
      NamespaceId.SYSTEM.getNamespace() : namespaceId.getEntityName();

    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.GET, String.format("namespaces/%s/artifact-internals/artifacts/%s/versions/%s/location",
                                      namespace, artifactInfo.getName(), artifactInfo.getVersion()));
    // add header if auth is enabled
    if (authorizationEnabled) {
      requestBuilder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }

    return createAndGetClassLoader(requestBuilder.build(), artifactInfo, parentClassLoader);
  }

  private List<ArtifactInfo> getArtifactsList(HttpRequest httpRequest) throws IOException {
    HttpResponse httpResponse = remoteClient.execute(httpRequest);

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      throw new IOException("Could not list artifacts, endpoint not found");
    }

    if (httpResponse.getResponseCode() == 200) {
      List<ArtifactInfo> artifactInfoList =
        GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_INFO_LIST_TYPE);
      return artifactInfoList;
    } else {
      throw new IOException(String.format("Exception while getting artifacts list %s",
                                          httpResponse.getResponseBodyAsString()));
    }
  }

  private CloseableClassLoader createAndGetClassLoader(HttpRequest httpRequest,
                                                       ArtifactInfo artifactInfo,
                                                       @Nullable ClassLoader parentClassLoader) throws IOException {
    HttpResponse httpResponse = remoteClient.execute(httpRequest);

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      throw new IOException("Could not get artifact detail, endpoint not found");
    }

    if (httpResponse.getResponseCode() == 200) {
      String path = httpResponse.getResponseBodyAsString();
      Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
      if (!location.exists()) {
        throw new IOException(String.format("Artifact Location does not exist %s for artifact %s version %s",
                                            path, artifactInfo.getName(), artifactInfo.getVersion()));
      }
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.unJar(location, unpackedDir);
      DirectoryClassLoader directoryClassLoader =
        new DirectoryClassLoader(unpackedDir,
                                 parentClassLoader == null ? bootstrapClassLoader : parentClassLoader, "lib");
      return new CloseableClassLoader(directoryClassLoader, new ClassLoaderCleanup(directoryClassLoader, unpackedDir));
    } else {
      throw new IOException(String.format("Exception while getting artifacts list %s",
                                          httpResponse.getResponseBodyAsString()));
    }
  }

  private static final class ClassLoaderCleanup implements Closeable {
    private final File directory;
    private final DirectoryClassLoader directoryClassLoader;

    private ClassLoaderCleanup(DirectoryClassLoader directoryClassLoader, File directory) {
      this.directoryClassLoader = directoryClassLoader;
      this.directory = directory;
    }
    @Override
    public void close() throws IOException {
      try {
        Closeables.closeQuietly(directoryClassLoader);
        DirUtils.deleteDirectoryContents(directory);
      } catch (IOException e) {
        LOG.warn("Failed to delete directory {}", directory, e);
      }
    }
  }
}
